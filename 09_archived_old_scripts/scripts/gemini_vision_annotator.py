#!/usr/bin/env python3
# scripts/gemini_vision_annotator.py
import os
import json
import argparse
import logging
import time
from pathlib import Path
from tqdm import tqdm
import google.generativeai as genai
from PIL import Image

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("gemini_annotation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configure the API
def setup_gemini_api(api_key):
    """Set up Gemini API with the provided key"""
    genai.configure(api_key=api_key)
    return genai.GenerativeModel('gemini-1.5-flash')

def analyze_image(model, image_path):
    """Analyze an image with Gemini 2.5 Flash to extract visible information"""
    try:
        logger.info(f"Analyzing image: {image_path}")
        
        # Open the image
        img = Image.open(image_path)
        
        # Define the prompt to extract only visually present information
        prompt = """
        Analyze this grocery product image and extract ONLY information that is VISUALLY PRESENT in the image.
        Return a JSON object with these fields: 
        - product_name: The name of the product as shown on the packaging
        - brand: The brand name visible on the packaging
        - amount: Any numerical quantity shown (e.g., 500, 1)
        - unit: The unit of measurement (e.g., g, kg, ml, L)
        
        IMPORTANT: Only include information you can actually see in the image. If you can't determine a field, use null.
        Return ONLY the JSON, no additional explanations.
        """
        
        # Generate content
        response = model.generate_content([prompt, img])
        
        # Parse the JSON from the response
        response_text = response.text
        
        # Extract JSON if embedded in a code block
        if "```json" in response_text:
            json_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            json_text = response_text.split("```")[1].split("```")[0].strip() 
        else:
            json_text = response_text.strip()
        
        # Parse JSON
        try:
            result = json.loads(json_text)
            logger.info(f"Successfully parsed result: {result}")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from response: {response_text}")
            logger.error(f"JSON error: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Error analyzing image {image_path}: {str(e)}")
        return None

def process_images(images_dir, output_file, api_key, max_images=50, delay=1):
    """Process a batch of images"""
    logger.info(f"Processing images from {images_dir}")
    
    # Create output directory
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Setup Gemini API
    model = setup_gemini_api(api_key)
    
    # Find all images
    image_paths = []
    for root, _, files in os.walk(images_dir):
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                image_paths.append(os.path.join(root, file))
    
    # Limit number of images if specified
    if max_images and len(image_paths) > max_images:
        image_paths = image_paths[:max_images]
    
    logger.info(f"Found {len(image_paths)} images, processing {len(image_paths)}")
    
    # Check for existing results and load them
    results = []
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            logger.info(f"Loaded {len(results)} existing results from {output_file}")
            
            # Get already processed product IDs
            processed_ids = set(item["product_id"] for item in results)
            
            # Filter out already processed images
            image_paths = [p for p in image_paths if os.path.splitext(os.path.basename(p))[0] not in processed_ids]
            logger.info(f"After filtering already processed items, {len(image_paths)} images remain to be processed")
        except Exception as e:
            logger.warning(f"Failed to load existing results: {e}")
    
    # Process each image
    for i, image_path in enumerate(tqdm(image_paths, desc="Analyzing Images")):
        # Add delay to avoid rate limiting
        if i > 0:
            time.sleep(delay)
        
        product_id = os.path.splitext(os.path.basename(image_path))[0]
        
        # Analyze image
        annotation = analyze_image(model, image_path)
        
        if annotation:
            # Add image info
            result = {
                "product_id": product_id,
                "image_path": image_path,
                "visible_info": annotation
            }
            
            results.append(result)
            
            # Save after each successful analysis to preserve progress
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
    
    # Export to CSV for easy viewing
    csv_file = os.path.splitext(output_file)[0] + ".csv"
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write("product_id,image_file,product_name,brand,amount,unit\n")
        
        for result in results:
            product_id = result["product_id"]
            image_file = os.path.basename(result["image_path"])
            info = result["visible_info"]
            
            product_name = info.get("product_name", "")
            brand = info.get("brand", "")
            amount = info.get("amount", "")
            unit = info.get("unit", "")
            
            f.write(f'"{product_id}","{image_file}","{product_name}","{brand}","{amount}","{unit}"\n')
    
    logger.info(f"Processed a total of {len(results)} images")
    logger.info(f"Annotations saved to {output_file}")
    logger.info(f"CSV exported to {csv_file}")
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Annotate product images using Gemini 2.5 Flash")
    parser.add_argument("--images-dir", required=True, help="Directory containing product images")
    parser.add_argument("--output-file", default="data/gemini_annotations.json", help="Output file for annotations")
    parser.add_argument("--api-key", required=True, help="Google AI API key")
    parser.add_argument("--max-images", type=int, default=50, help="Maximum number of images to process")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between API requests in seconds")
    
    args = parser.parse_args()
    
    process_images(args.images_dir, args.output_file, args.api_key, args.max_images, args.delay)