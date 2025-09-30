#!/usr/bin/env python3
"""
Fixed Product Recognition Pipeline
Uses Tesseract OCR + CLIP (openai/clip-vit-base-patch32) for reliable product identification
"""

import os
import torch
from PIL import Image
import numpy as np
from pathlib import Path
import logging
from typing import List, Dict, Tuple, Optional
import json
import cv2
from dataclasses import dataclass
import clip
import faiss
from sklearn.metrics.pairwise import cosine_similarity
import sqlite3
from datetime import datetime
import re
import pytesseract
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy import ndimage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'mps' if torch.backends.mps.is_available() else 'cpu')
logger.info(f"Using device: {device}")

@dataclass
class ProductMatch:
    """Result of product recognition"""
    product_id: str
    confidence: float
    match_type: str  # 'text', 'visual', 'hybrid'
    category: str
    name: str
    brand: Optional[str] = None
    price: Optional[float] = None
    ocr_text: Optional[str] = None
    visual_similarity: Optional[float] = None
    text_similarity: Optional[float] = None

class TesseractOCR:
    """Advanced Tesseract OCR with label detection for product brand/name extraction"""
    
    def __init__(self):
        # Optimized PSM modes for Hebrew product labels (based on 91-96% confidence results)
        self.psm_modes = [6, 3, 8, 11]  # 6=uniform block (best), 3=auto, 8=single word, 11=sparse
        # Use LSTM neural network (--oem 3) which excels at Hebrew text recognition
        self.base_config = '--oem 3 -l heb+eng'
        # Minimum confidence threshold for high-quality results
        self.min_confidence = 70
        # Label detection parameters
        self.label_detection_enabled = True
        logger.info("Hebrew-optimized Tesseract OCR with label detection initialized")
    
    def _preprocess_image(self, image: np.ndarray, method: str) -> np.ndarray:
        """Apply Hebrew-optimized preprocessing method to image"""
        if method == "high_contrast":
            # High contrast black/white - optimal for Hebrew text (91-96% confidence)
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return thresh
        
        elif method == "inverted_high_contrast":
            # Inverted high contrast - for white text on dark backgrounds
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
            return thresh
        
        elif method == "adaptive_gaussian":
            # Adaptive threshold for varying lighting conditions
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            return cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
        
        elif method == "enhanced_contrast":
            # Enhance contrast before thresholding
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            enhanced = clahe.apply(gray)
            _, thresh = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return thresh
        
        elif method == "denoised":
            # Denoise while maintaining text clarity
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            denoised = cv2.fastNlMeansDenoising(gray)
            _, thresh = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return thresh
        
        elif method == "original":
            # Clean grayscale for well-lit, clear images
            return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        else:
            return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    def _detect_text_regions(self, image: np.ndarray) -> List[Tuple[int, int, int, int]]:
        """Detect text regions using MSER (Maximally Stable Extremal Regions)"""
        try:
            # Convert to grayscale if needed
            if len(image.shape) == 3:
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            else:
                gray = image.copy()
            
            # Create MSER detector optimized for text
            mser = cv2.MSER_create()
            
            # Detect regions
            regions, _ = mser.detectRegions(gray)
            
            # Convert regions to bounding boxes
            text_regions = []
            for region in regions:
                # Get bounding rectangle
                x, y, w, h = cv2.boundingRect(region.reshape(-1, 1, 2))
                
                # Filter by aspect ratio and size (typical for product labels)
                aspect_ratio = w / h if h > 0 else 0
                area = w * h
                
                # Keep regions that look like text labels
                if (0.2 <= aspect_ratio <= 8.0 and  # Text-like aspect ratio
                    100 <= area <= 10000 and         # Reasonable size
                    h >= 15 and w >= 30):            # Minimum dimensions
                    text_regions.append((x, y, w, h))
            
            # Remove overlapping regions (keep larger ones)
            text_regions = self._filter_overlapping_regions(text_regions)
            
            # Sort by area (largest first) and take top regions
            text_regions.sort(key=lambda r: r[2] * r[3], reverse=True)
            
            return text_regions[:8]  # Limit to top 8 text regions
            
        except Exception as e:
            logger.warning(f"Text region detection failed: {e}")
            return []
    
    def _filter_overlapping_regions(self, regions: List[Tuple[int, int, int, int]]) -> List[Tuple[int, int, int, int]]:
        """Remove overlapping regions, keeping larger ones"""
        if not regions:
            return []
        
        # Sort by area (largest first)
        regions = sorted(regions, key=lambda r: r[2] * r[3], reverse=True)
        
        filtered = []
        for current in regions:
            x1, y1, w1, h1 = current
            
            # Check if this region overlaps significantly with any already filtered region
            overlaps = False
            for existing in filtered:
                x2, y2, w2, h2 = existing
                
                # Calculate overlap
                overlap_x = max(0, min(x1 + w1, x2 + w2) - max(x1, x2))
                overlap_y = max(0, min(y1 + h1, y2 + h2) - max(y1, y2))
                overlap_area = overlap_x * overlap_y
                
                current_area = w1 * h1
                overlap_ratio = overlap_area / current_area if current_area > 0 else 0
                
                if overlap_ratio > 0.3:  # 30% overlap threshold
                    overlaps = True
                    break
            
            if not overlaps:
                filtered.append(current)
        
        return filtered
    
    def _detect_prominent_labels(self, image: np.ndarray) -> List[Tuple[int, int, int, int]]:
        """Detect prominent text labels using edge detection and contours"""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if len(image.shape) == 3 else image.copy()
            
            # Apply Gaussian blur to reduce noise
            blurred = cv2.GaussianBlur(gray, (5, 5), 0)
            
            # Edge detection
            edges = cv2.Canny(blurred, 50, 150, apertureSize=3)
            
            # Morphological operations to connect text components
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            edges = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel)
            
            # Find contours
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            label_regions = []
            for contour in contours:
                x, y, w, h = cv2.boundingRect(contour)
                
                # Filter contours that could be text labels
                aspect_ratio = w / h if h > 0 else 0
                area = w * h
                
                if (1.0 <= aspect_ratio <= 6.0 and    # Text-like aspect ratio
                    200 <= area <= 8000 and           # Reasonable size for labels
                    h >= 20 and w >= 40):             # Minimum dimensions
                    label_regions.append((x, y, w, h))
            
            # Remove overlapping regions
            label_regions = self._filter_overlapping_regions(label_regions)
            
            # Sort by area and position (prefer larger, more central regions)
            image_center_y = image.shape[0] // 2
            label_regions.sort(key=lambda r: (r[2] * r[3], -abs(r[1] + r[3]//2 - image_center_y)), reverse=True)
            
            return label_regions[:5]  # Top 5 label regions
            
        except Exception as e:
            logger.warning(f"Prominent label detection failed: {e}")
            return []
    
    def _extract_from_region(self, image: np.ndarray, region: Tuple[int, int, int, int]) -> Dict:
        """Extract text from a specific region"""
        x, y, w, h = region
        
        # Extract region with padding
        padding = 5
        x_start = max(0, x - padding)
        y_start = max(0, y - padding)
        x_end = min(image.shape[1], x + w + padding)
        y_end = min(image.shape[0], y + h + padding)
        
        region_image = image[y_start:y_end, x_start:x_end]
        
        if region_image.size == 0:
            return {"text": "", "confidence": 0, "words": [], "word_count": 0}
        
        # Try different preprocessing methods on this region
        preprocessing_methods = ["high_contrast", "enhanced_contrast", "original"]
        
        best_result = {"text": "", "confidence": 0, "words": [], "word_count": 0}
        
        for method in preprocessing_methods:
            processed_region = self._preprocess_image(
                region_image if len(region_image.shape) == 3 else cv2.cvtColor(region_image, cv2.COLOR_GRAY2BGR), 
                method
            )
            
            # Try PSM modes optimized for single text blocks
            for psm_mode in [6, 8, 7]:  # 6=uniform block, 8=single word, 7=single text line
                result = self._extract_with_config(processed_region, psm_mode)
                
                # Score this result
                score = result["confidence"] * 0.8 + result["word_count"] * 10
                best_score = best_result["confidence"] * 0.8 + best_result["word_count"] * 10
                
                if score > best_score and result["text"].strip():
                    best_result = result
        
        return best_result
    
    def _filter_relevant_text(self, text: str, words: List[str]) -> Tuple[str, List[str]]:
        """Filter text to keep only brand/product relevant information"""
        if not text or not words:
            return "", []
        
        # Known Hebrew product/brand words to prioritize
        priority_hebrew_words = [
            'במבה', 'נסקפה', 'האגיס', 'שופרסל', 'אסם', 'תנובה', 'דנונה', 'יטבתה',
            'עלית', 'סנו', 'חלב', 'קפה', 'חיתולים', 'קרקר', 'תפוח', 'אדמה'
        ]
        
        # Known English brands
        priority_english_brands = [
            'NESCAFE', 'HUGGIES', 'PAMPERS', 'COCA', 'PEPSI', 'NESTLE', 'UNILEVER',
            'Tasters', 'Choice', 'Freedom', 'Dry'
        ]
        
        # Filter words
        filtered_words = []
        for word in words:
            word = word.strip()
            if len(word) < 2:  # Skip very short words
                continue
            
            # Skip pure numbers, prices, and symbols
            if (re.match(r'^[\d\s₪%=\|\-\+\*\/#]+$', word) or
                re.match(r'^\d+$', word) or
                word in ['/', '|', '-', '+', '*', '#', '=', '%']):
                continue
            
            # Skip common marketing words
            marketing_words = ['של', 'עם', 'בטעם', 'ללא', 'חדש', 'משופר', 'איכות', 'טעים', 'נהדר',
                              'NEW', 'IMPROVED', 'QUALITY', 'FRESH', 'BEST', 'PREMIUM', 'נוחות', 'בכל', 'תנועה']
            if word in marketing_words:
                continue
            
            # Prioritize known brands/products
            if (word in priority_hebrew_words or 
                word in priority_english_brands or
                word.upper() in priority_english_brands):
                filtered_words.append(word)
                continue
            
            # Keep Hebrew words (2+ characters)
            if re.match(r'^[א-ת]{2,}$', word):
                filtered_words.append(word)
                continue
                
            # Keep English capitalized words (likely brands)
            if re.match(r'^[A-Z][a-zA-Z]{1,}$', word):
                filtered_words.append(word)
                continue
                
            # Keep all caps English (likely brands)
            if re.match(r'^[A-Z]{2,}$', word):
                filtered_words.append(word)
                continue
                
            # Keep words with numbers and Hebrew units (weight/volume)
            if re.match(r'^\d+[מלגק"ק]+$', word):
                filtered_words.append(word)
                continue
        
        # Remove duplicates while preserving order
        seen = set()
        unique_filtered_words = []
        for word in filtered_words:
            if word not in seen:
                seen.add(word)
                unique_filtered_words.append(word)
        
        # Reconstruct filtered text
        filtered_text = ' '.join(unique_filtered_words)
        
        return filtered_text, unique_filtered_words
    
    def _extract_with_config(self, processed_image: np.ndarray, psm_mode: int) -> Dict:
        """Extract text with specific PSM mode"""
        config = f"{self.base_config} --psm {psm_mode}"
        
        try:
            # Extract text
            text = pytesseract.image_to_string(processed_image, config=config)
            
            # Get detailed data for confidence calculation
            data = pytesseract.image_to_data(processed_image, config=config, output_type=pytesseract.Output.DICT)
            
            # Calculate confidence
            confidences = [int(conf) for conf in data['conf'] if int(conf) > 0]
            avg_confidence = np.mean(confidences) if confidences else 0
            
            # Extract high-confidence words (raised threshold for Hebrew accuracy)
            words = []
            for i, conf in enumerate(data['conf']):
                if int(conf) > self.min_confidence and data['text'][i].strip():
                    words.append(data['text'][i].strip())
            
            return {
                "text": text.strip(),
                "confidence": avg_confidence,
                "words": words,
                "word_count": len(words)
            }
            
        except Exception as e:
            return {"text": "", "confidence": 0, "words": [], "word_count": 0}
    
    def extract_text(self, image_path: str) -> Dict:
        """Extract text using label detection and focused OCR"""
        try:
            image = cv2.imread(image_path)
            if image is None:
                return {"text": "", "confidence": 0, "words": [], "method": "none", "psm": 0}
            
            all_results = []
            
            if self.label_detection_enabled:
                # Method 1: Extract from detected text regions (MSER)
                text_regions = self._detect_text_regions(image)
                for i, region in enumerate(text_regions):
                    result = self._extract_from_region(image, region)
                    if result["text"].strip():
                        result["method"] = f"mser_region_{i}"
                        result["region"] = region
                        all_results.append(result)
                
                # Method 2: Extract from prominent labels (contour-based)
                label_regions = self._detect_prominent_labels(image)
                for i, region in enumerate(label_regions):
                    result = self._extract_from_region(image, region)
                    if result["text"].strip():
                        result["method"] = f"contour_region_{i}"
                        result["region"] = region
                        all_results.append(result)
            
            # Method 3: Always try full image OCR as well (for comparison)
            logger.info("Adding full image OCR results")
            preprocessing_methods = ["high_contrast", "enhanced_contrast", "original"]
            
            for method in preprocessing_methods:
                processed_image = self._preprocess_image(image, method)
                
                for psm_mode in [6, 3]:  # Focus on best PSM modes
                    result = self._extract_with_config(processed_image, psm_mode)
                    if result["text"].strip():
                        result["method"] = f"fullimage_{method}"
                        result["psm"] = psm_mode
                        all_results.append(result)
            
            if not all_results:
                return {"text": "", "confidence": 0, "words": [], "method": "none", "psm": 0}
            
            # Score all results, applying filtering first to see which has better brand content
            scored_results = []
            for result in all_results:
                # Apply filtering to see what we get
                filtered_text, filtered_words = self._filter_relevant_text(result["text"], result["words"])
                
                # Score based on:
                # - OCR confidence (40%)
                # - Number of filtered meaningful words (40%) 
                # - Presence of known brands (20%)
                brand_bonus = 0
                priority_hebrew_words = ['במבה', 'נסקפה', 'האגיס', 'שופרסל', 'אסם']
                priority_english_brands = ['NESCAFE', 'HUGGIES', 'Tasters', 'Choice', 'Freedom']
                
                for word in filtered_words:
                    if word in priority_hebrew_words or word in priority_english_brands:
                        brand_bonus += 20
                
                score = (result["confidence"] * 0.4 + 
                        len(filtered_words) * 10 + 
                        brand_bonus)
                
                scored_results.append({
                    'result': result,
                    'score': score,
                    'filtered_text': filtered_text,
                    'filtered_words': filtered_words
                })
            
            # Find best scored result
            best_scored = max(scored_results, key=lambda x: x['score'])
            best_result = best_scored['result']
            filtered_text = best_scored['filtered_text']
            filtered_words = best_scored['filtered_words']
            
            # We already have filtered text from scoring, no need to filter again
            
            # Update result with filtered text
            final_result = {
                "text": filtered_text,
                "confidence": best_result["confidence"],
                "words": filtered_words,
                "word_count": len(filtered_words),
                "method": best_result["method"],
                "psm": best_result.get("psm", 0),
                "original_text": best_result["text"],  # Keep original for debugging
                "original_words": best_result["words"],
                "regions_detected": len(text_regions) + len(label_regions) if self.label_detection_enabled else 0
            }
            
            if final_result["text"]:
                logger.debug(f"OCR success: method={final_result['method']}, conf={final_result['confidence']:.1f}, filtered_words={len(filtered_words)}")
            
            return final_result
            
        except Exception as e:
            logger.error(f"OCR error: {e}")
            return {"text": "", "confidence": 0, "words": [], "method": "error", "psm": 0}
    
    def visualize_detected_regions(self, image_path: str, output_path: str = None) -> str:
        """Visualize detected text regions for debugging"""
        try:
            image = cv2.imread(image_path)
            if image is None:
                return "Failed to load image"
            
            # Make a copy for drawing
            vis_image = image.copy()
            
            # Detect regions
            text_regions = self._detect_text_regions(image)
            label_regions = self._detect_prominent_labels(image)
            
            # Draw text regions in blue
            for i, (x, y, w, h) in enumerate(text_regions):
                cv2.rectangle(vis_image, (x, y), (x + w, y + h), (255, 0, 0), 2)
                cv2.putText(vis_image, f'T{i}', (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 0, 0), 1)
            
            # Draw label regions in green
            for i, (x, y, w, h) in enumerate(label_regions):
                cv2.rectangle(vis_image, (x, y), (x + w, y + h), (0, 255, 0), 2)
                cv2.putText(vis_image, f'L{i}', (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 1)
            
            # Save visualization
            if output_path is None:
                output_path = image_path.replace('.jpg', '_regions.jpg')
            
            cv2.imwrite(output_path, vis_image)
            
            return f"Visualization saved: {output_path}. Text regions: {len(text_regions)}, Label regions: {len(label_regions)}"
            
        except Exception as e:
            return f"Visualization error: {e}"
    
    def create_test_image(self, text: str, output_path: str, size: tuple = (400, 100)) -> bool:
        """Create test image with optimal settings for Hebrew text (as per guide)"""
        try:
            from PIL import Image, ImageDraw, ImageFont
            
            # Create image with white background (RGB: 255, 255, 255)
            img = Image.new('RGB', size, color='white')
            draw = ImageDraw.Draw(img)
            
            # Use black text (RGB: 0, 0, 0) for maximum contrast
            # Position with adequate margins (50px as recommended)
            margin = 50
            y_position = size[1] // 2 - 10  # Center vertically
            draw.text((margin, y_position), text, fill='black')
            
            img.save(output_path)
            logger.info(f"Test image created: {output_path} with text '{text}'")
            return True
            
        except Exception as e:
            logger.error(f"Error creating test image: {e}")
            return False

class CLIPEncoder:
    """CLIP vision encoder for product images"""
    
    def __init__(self):
        logger.info("Loading CLIP model: openai/clip-vit-base-patch32")
        self.model, self.preprocess = clip.load("ViT-B/32", device=device)
        self.model.eval()
        logger.info("CLIP model loaded successfully")
    
    def encode_image(self, image_path: str) -> np.ndarray:
        """Encode image to feature vector"""
        try:
            image = Image.open(image_path).convert('RGB')
            image_tensor = self.preprocess(image).unsqueeze(0).to(device)
            
            with torch.no_grad():
                features = self.model.encode_image(image_tensor)
                features = features / features.norm(dim=-1, keepdim=True)  # Normalize
            
            return features.cpu().numpy().flatten()
            
        except Exception as e:
            logger.error(f"Image encoding error: {e}")
            return np.zeros(512)  # CLIP ViT-B/32 produces 512-dim features
    
    def encode_text(self, text: str) -> np.ndarray:
        """Encode text to feature vector"""
        try:
            text_tokens = clip.tokenize([text]).to(device)
            
            with torch.no_grad():
                features = self.model.encode_text(text_tokens)
                features = features / features.norm(dim=-1, keepdim=True)  # Normalize
            
            return features.cpu().numpy().flatten()
            
        except Exception as e:
            logger.error(f"Text encoding error: {e}")
            return np.zeros(512)

class ProductDatabase:
    """Simple product database with FAISS indexing"""
    
    def __init__(self, db_path: str = "products_complete.db"):
        self.db_path = db_path
        self.clip_encoder = CLIPEncoder()
        self.ocr = TesseractOCR()
        
        # Initialize FAISS indices
        self.visual_index = None
        self.products = []
        
        # Text vectorizer for OCR matching
        self.text_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words=None,  # Keep Hebrew words
            ngram_range=(1, 2)
        )
        
        self._load_database()
        logger.info(f"Product database initialized: {db_path}")
    
    def _load_database(self):
        """Load products and build indices"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT id, name_he, brand, price FROM products')
        db_products = cursor.fetchall()
        conn.close()
        
        logger.info(f"Processing {len(db_products)} products for indexing...")
        
        visual_features = []
        text_features = []
        
        for i, (product_id, name_he, brand, price) in enumerate(db_products):
            # Find product images
            image_dir = Path(f"data/products/{product_id}")
            if image_dir.exists():
                image_files = list(image_dir.glob("*.jpg"))
                if image_files:
                    # Use first image for encoding
                    image_path = str(image_files[0])
                    
                    # Encode image
                    visual_feat = self.clip_encoder.encode_image(image_path)
                    visual_features.append(visual_feat)
                    
                    # Create text representation
                    text_repr = f"{name_he or ''} {brand or ''}".strip()
                    text_features.append(text_repr)
                    
                    # Store product info
                    self.products.append({
                        'id': product_id,
                        'name': name_he,
                        'brand': brand,
                        'price': price,
                        'text': text_repr
                    })
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"Processed {i + 1} products...")
        
        if visual_features:
            # Build FAISS index for visual features
            visual_features = np.array(visual_features).astype('float32')
            self.visual_index = faiss.IndexFlatIP(visual_features.shape[1])  # Inner product (cosine similarity)
            self.visual_index.add(visual_features)
            
            # Build text vectorizer
            if text_features:
                self.text_vectorizer.fit(text_features)
                self.text_features_matrix = self.text_vectorizer.transform(text_features)
            
            logger.info(f"Indexed {len(self.products)} products with visual and text features")
        else:
            logger.warning("No products with images found!")
    
    def search_visual(self, image_path: str, k: int = 5) -> List[Tuple[str, float]]:
        """Search by visual similarity"""
        if self.visual_index is None:
            return []
        
        query_features = self.clip_encoder.encode_image(image_path)
        query_features = query_features.reshape(1, -1).astype('float32')
        
        similarities, indices = self.visual_index.search(query_features, k)
        
        results = []
        for sim, idx in zip(similarities[0], indices[0]):
            if idx < len(self.products):
                results.append((self.products[idx]['id'], float(sim)))
        
        return results
    
    def search_text(self, ocr_text: str, k: int = 5) -> List[Tuple[str, float]]:
        """Search by text similarity"""
        if not ocr_text.strip() or not hasattr(self, 'text_features_matrix'):
            return []
        
        query_vector = self.text_vectorizer.transform([ocr_text])
        similarities = cosine_similarity(query_vector, self.text_features_matrix).flatten()
        
        # Get top-k indices
        top_indices = np.argsort(similarities)[::-1][:k]
        
        results = []
        for idx in top_indices:
            if idx < len(self.products):
                results.append((self.products[idx]['id'], float(similarities[idx])))
        
        return results
    
    def get_product_info(self, product_id: str) -> Dict:
        """Get product information by ID"""
        for product in self.products:
            if product['id'] == product_id:
                return product
        return {}

class FixedProductRecognizer:
    """Fixed product recognition pipeline"""
    
    def __init__(self, db_path: str = "products_complete.db"):
        logger.info("Initializing Fixed Product Recognition Pipeline...")
        self.database = ProductDatabase(db_path)
        self.ocr = TesseractOCR()
        logger.info("Fixed Product Recognition Pipeline ready")
    
    def recognize_product(self, image_path: str) -> ProductMatch:
        """Recognize product from image"""
        logger.info(f"Recognizing product from: {image_path}")
        
        try:
            # Extract text with OCR
            ocr_result = self.ocr.extract_text(image_path)
            ocr_text = ocr_result["text"]
            ocr_confidence = ocr_result["confidence"]
            
            # Search by visual similarity
            visual_matches = self.database.search_visual(image_path, k=5)
            
            # Search by text similarity
            text_matches = self.database.search_text(ocr_text, k=5)
            
            # Determine best match
            best_match = self._determine_best_match(visual_matches, text_matches, ocr_confidence)
            
            # Get product info
            product_info = self.database.get_product_info(best_match['product_id'])
            
            # Enhanced OCR info for debugging
            ocr_info = f"{ocr_text}"
            if 'method' in ocr_result:
                ocr_info += f" (method: {ocr_result['method']}, psm: {ocr_result['psm']}, conf: {ocr_confidence:.1f})"
            
            return ProductMatch(
                product_id=best_match['product_id'],
                confidence=best_match['confidence'],
                match_type=best_match['match_type'],
                category=product_info.get('category', ''),
                name=product_info.get('name', 'Unknown'),
                brand=product_info.get('brand'),
                price=product_info.get('price'),
                ocr_text=ocr_info,
                visual_similarity=best_match.get('visual_similarity'),
                text_similarity=best_match.get('text_similarity')
            )
            
        except Exception as e:
            logger.error(f"Recognition error: {e}")
            return ProductMatch(
                product_id="unknown",
                confidence=0.1,
                match_type="error",
                category="",
                name="Error in recognition",
                ocr_text="",
                visual_similarity=None,
                text_similarity=None
            )
    
    def _determine_best_match(self, visual_matches, text_matches, ocr_confidence):
        """Determine the best match from visual and text results"""
        
        # If no matches found
        if not visual_matches and not text_matches:
            return {
                'product_id': 'unknown',
                'confidence': 0.1,
                'match_type': 'none'
            }
        
        # If only visual matches
        if visual_matches and not text_matches:
            best_visual = visual_matches[0]
            return {
                'product_id': best_visual[0],
                'confidence': best_visual[1],
                'match_type': 'visual',
                'visual_similarity': best_visual[1]
            }
        
        # If only text matches
        if text_matches and not visual_matches:
            best_text = text_matches[0]
            return {
                'product_id': best_text[0],
                'confidence': best_text[1],
                'match_type': 'text',
                'text_similarity': best_text[1]
            }
        
        # If both visual and text matches
        best_visual = visual_matches[0]
        best_text = text_matches[0]
        
        # Weight visual vs text based on OCR confidence
        ocr_weight = min(ocr_confidence / 100.0, 0.8)  # Cap at 0.8
        visual_weight = 1.0 - ocr_weight
        
        visual_score = best_visual[1] * visual_weight
        text_score = best_text[1] * ocr_weight
        
        if visual_score > text_score:
            return {
                'product_id': best_visual[0],
                'confidence': float(visual_score),
                'match_type': 'hybrid_visual',
                'visual_similarity': best_visual[1],
                'text_similarity': best_text[1] if text_matches else None
            }
        else:
            return {
                'product_id': best_text[0],
                'confidence': float(text_score),
                'match_type': 'hybrid_text',
                'visual_similarity': best_visual[1] if visual_matches else None,
                'text_similarity': best_text[1]
            }

class TrainingDataProcessor:
    """Process training data from JSONL and map to images"""
    
    def __init__(self, jsonl_path: str, image_dirs: List[str]):
        self.jsonl_path = jsonl_path
        self.image_dirs = image_dirs
        self.training_items = []
        logger.info(f"Initializing TrainingDataProcessor with {len(image_dirs)} image directories")
    
    def load_training_data(self) -> List[Dict]:
        """Load training data from JSONL file"""
        try:
            with open(self.jsonl_path, 'r', encoding='utf-8') as f:
                for line in f:
                    item = json.loads(line.strip())
                    self.training_items.append(item)
            
            logger.info(f"Loaded {len(self.training_items)} training items from {self.jsonl_path}")
            return self.training_items
            
        except Exception as e:
            logger.error(f"Error loading training data: {e}")
            return []
    
    def validate_image_paths(self) -> Dict[str, int]:
        """Validate that image paths exist and return statistics"""
        stats = {
            'total': len(self.training_items),
            'valid_paths': 0,
            'missing_files': 0,
            'by_store': {}
        }
        
        for item in self.training_items:
            store = item.get('source_supermarket', 'unknown')
            if store not in stats['by_store']:
                stats['by_store'][store] = {'total': 0, 'valid': 0, 'missing': 0}
            
            stats['by_store'][store]['total'] += 1
            
            image_path = item.get('local_image_path', '')
            if os.path.exists(image_path):
                stats['valid_paths'] += 1
                stats['by_store'][store]['valid'] += 1
            else:
                stats['missing_files'] += 1
                stats['by_store'][store]['missing'] += 1
                logger.warning(f"Missing image: {image_path}")
        
        logger.info(f"Image validation: {stats['valid_paths']}/{stats['total']} valid paths")
        return stats
    
    def get_items_by_store(self, store_name: str) -> List[Dict]:
        """Get training items for a specific store"""
        return [item for item in self.training_items if item.get('source_supermarket', '').lower() == store_name.lower()]
    
    def process_batch(self, batch_size: int = 10) -> List[Dict]:
        """Process training items in batches for efficient handling"""
        for i in range(0, len(self.training_items), batch_size):
            batch = self.training_items[i:i+batch_size]
            yield batch

def create_training_index(training_data_path: str, image_directories: List[str]) -> None:
    """Create an index from training data for product recognition"""
    
    processor = TrainingDataProcessor(training_data_path, image_directories)
    
    # Load training data
    items = processor.load_training_data()
    if not items:
        logger.error("No training data loaded")
        return
    
    # Validate image paths
    stats = processor.validate_image_paths()
    print(f"\nTraining Data Statistics:")
    print(f"{'='*50}")
    print(f"Total items: {stats['total']}")
    print(f"Valid image paths: {stats['valid_paths']}")
    print(f"Missing files: {stats['missing_files']}")
    print(f"Success rate: {stats['valid_paths']/stats['total']*100:.1f}%")
    
    print(f"\nBy Store:")
    for store, store_stats in stats['by_store'].items():
        print(f"  {store}: {store_stats['valid']}/{store_stats['total']} valid ({store_stats['valid']/store_stats['total']*100:.1f}%)")
    
    # Initialize CLIP encoder for feature extraction
    clip_encoder = CLIPEncoder()
    
    # Process items in batches
    print(f"\nProcessing items for indexing...")
    
    indexed_items = []
    ocr = TesseractOCR()
    
    for i, batch in enumerate(processor.process_batch(batch_size=5)):
        print(f"Processing batch {i+1}...")
        
        for item in batch:
            image_path = item.get('local_image_path', '')
            if not os.path.exists(image_path):
                continue
            
            try:
                # Extract visual features
                visual_features = clip_encoder.encode_image(image_path)
                
                # Extract text features from image (OCR)
                ocr_result = ocr.extract_text(image_path)
                
                # Create indexed item
                indexed_item = {
                    'training_item_id': item.get('training_item_id', ''),
                    'source_supermarket': item.get('source_supermarket', ''),
                    'original_store_product_id': item.get('original_store_product_id', ''),
                    'text_for_embedding': item.get('text_for_embedding', ''),
                    'local_image_path': image_path,
                    'visual_features': visual_features.tolist(),
                    'ocr_text': ocr_result.get('text', ''),
                    'ocr_confidence': ocr_result.get('confidence', 0),
                    'barcode_ean13': item.get('barcode_ean13'),
                    'categories': item.get('_raw_categories_he', [])
                }
                
                indexed_items.append(indexed_item)
                
            except Exception as e:
                logger.error(f"Error processing {image_path}: {e}")
                continue
    
    # Save indexed data
    output_path = "training_index.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(indexed_items, f, ensure_ascii=False, indent=2)
    
    print(f"\nIndexed {len(indexed_items)} items saved to {output_path}")
    
    return indexed_items

def search_similar_products(query_image_path: str, index_path: str = "training_index.json", top_k: int = 5) -> List[Dict]:
    """Search for similar products using the training index"""
    
    # Load index
    try:
        with open(index_path, 'r', encoding='utf-8') as f:
            indexed_items = json.load(f)
        logger.info(f"Loaded index with {len(indexed_items)} items")
    except Exception as e:
        logger.error(f"Error loading index: {e}")
        return []
    
    # Initialize CLIP encoder
    clip_encoder = CLIPEncoder()
    
    # Extract features from query image
    query_features = clip_encoder.encode_image(query_image_path)
    
    # Calculate similarities
    similarities = []
    for item in indexed_items:
        stored_features = np.array(item['visual_features'])
        similarity = cosine_similarity([query_features], [stored_features])[0][0]
        
        similarities.append({
            'item': item,
            'similarity': float(similarity)
        })
    
    # Sort by similarity and return top-k
    similarities.sort(key=lambda x: x['similarity'], reverse=True)
    
    return similarities[:top_k]

def main():
    """Main function with enhanced functionality"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python fixed_product_recognition.py <image_path>              - Recognize single product")
        print("  python fixed_product_recognition.py --create-index           - Create training index")
        print("  python fixed_product_recognition.py --search <image_path>    - Search similar products")
        sys.exit(1)
    
    if sys.argv[1] == "--create-index":
        # Create training index
        training_data_path = "/Users/noa/Desktop/PriceComparisonApp/training_data.jsonl"
        image_directories = [
            "/Users/noa/Desktop/PriceComparisonApp/rami_levi_product_images",
            "/Users/noa/Desktop/PriceComparisonApp/shufersal_product_images", 
            "/Users/noa/Desktop/PriceComparisonApp/victory_product_images",
            "/Users/noa/Desktop/PriceComparisonApp/hatzi_hinam_product_images",
            "/Users/noa/Desktop/PriceComparisonApp/yochananof_product_images"
        ]
        
        create_training_index(training_data_path, image_directories)
        
    elif sys.argv[1] == "--search":
        if len(sys.argv) != 3:
            print("Usage: python fixed_product_recognition.py --search <image_path>")
            sys.exit(1)
            
        query_image = sys.argv[2]
        results = search_similar_products(query_image)
        
        print(f"\nSimilar Products for: {query_image}")
        print(f"{'='*60}")
        
        for i, result in enumerate(results, 1):
            item = result['item']
            similarity = result['similarity']
            
            print(f"\n{i}. Similarity: {similarity:.3f}")
            print(f"   Store: {item['source_supermarket']}")
            print(f"   Product ID: {item['training_item_id']}")
            print(f"   Description: {item['text_for_embedding']}")
            print(f"   OCR Text: {item['ocr_text']}")
            print(f"   Image: {item['local_image_path']}")
            
    else:
        # Original single image recognition
        image_path = sys.argv[1]
        
        # Initialize recognizer
        recognizer = FixedProductRecognizer()
        
        # Recognize product
        result = recognizer.recognize_product(image_path)
        
        # Print results
        print(f"\nProduct Recognition Results:")
        print(f"{'='*50}")
        print(f"Product ID: {result.product_id}")
        print(f"Name: {result.name}")
        print(f"Brand: {result.brand}")
        print(f"Price: ₪{result.price}")
        print(f"Confidence: {result.confidence:.3f}")
        print(f"Match Type: {result.match_type}")
        print(f"Visual Similarity: {result.visual_similarity}")
        print(f"Text Similarity: {result.text_similarity}")
        print(f"OCR Text: {result.ocr_text}")

if __name__ == "__main__":
    main()