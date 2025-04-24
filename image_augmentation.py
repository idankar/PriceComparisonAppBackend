# image_augmentation.py
import os
import cv2
import numpy as np
import random
from tqdm import tqdm
import logging
import argparse
import glob
from concurrent.futures import ProcessPoolExecutor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("augmentation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def augment_image(img):
    """
    Create variants of the image with different transformations to improve model robustness
    """
    augmented = []
    
    # Original image
    augmented.append(("original", img))
    
    # Rotate slightly
    for angle in [-5, 5]:
        h, w = img.shape[:2]
        M = cv2.getRotationMatrix2D((w/2, h/2), angle, 1)
        rotated = cv2.warpAffine(img, M, (w, h), borderMode=cv2.BORDER_REPLICATE)
        augmented.append((f"rotate_{angle}", rotated))
    
    # Add slight perspective transformation
    h, w = img.shape[:2]
    pts1 = np.float32([[0,0], [w,0], [0,h], [w,h]])
    pts2 = np.float32([[0,0], [w,0], [15,h-15], [w-15,h-15]])
    M = cv2.getPerspectiveTransform(pts1, pts2)
    warped = cv2.warpPerspective(img, M, (w, h), borderMode=cv2.BORDER_REPLICATE)
    augmented.append(("perspective", warped))
    
    # Change brightness
    bright = cv2.convertScaleAbs(img, alpha=1.3, beta=20)
    dark = cv2.convertScaleAbs(img, alpha=0.7, beta=-20)
    augmented.append(("bright", bright))
    augmented.append(("dark", dark))
    
    # Add slight blur
    blurred = cv2.GaussianBlur(img, (3, 3), 0)
    augmented.append(("blur", blurred))
    
    # Add noise
    noise = np.copy(img)
    noise_factor = 0.05
    noise = np.clip(noise + noise_factor * np.random.randn(*noise.shape), 0, 255).astype(np.uint8)
    augmented.append(("noise", noise))
    
    # Crop slightly
    h, w = img.shape[:2]
    crop_percent = 0.1
    crop_h, crop_w = int(h * crop_percent), int(w * crop_percent)
    cropped = img[crop_h:h-crop_h, crop_w:w-crop_w]
    # Resize back to original dimensions
    cropped = cv2.resize(cropped, (w, h))
    augmented.append(("crop", cropped))
    
    return augmented

def process_image(args):
    """Process a single image with augmentations (for parallel processing)"""
    img_path, aug_dir = args
    
    try:
        # Read the image
        img = cv2.imread(img_path)
        
        if img is None:
            logger.warning(f"Could not read image: {img_path}")
            return 0
        
        # Get base filename
        filename = os.path.basename(img_path)
        name, ext = os.path.splitext(filename)
        
        # Create augmented versions
        augmented_images = augment_image(img)
        
        # Save augmented images
        for aug_type, aug_img in augmented_images[1:]: # Skip the first one (original)
            aug_filename = f"{name}_{aug_type}{ext}"
            aug_path = os.path.join(aug_dir, aug_filename)
            cv2.imwrite(aug_path, aug_img)
        
        return len(augmented_images) - 1
    except Exception as e:
        logger.error(f"Error processing {img_path}: {str(e)}")
        return 0

def process_all_images(input_dir="data/images", output_dir="data/augmented", num_workers=4):
    """
    Augment all product images in the dataset
    
    Args:
        input_dir (str): Directory containing images organized by query
        output_dir (str): Directory to save augmented images
        num_workers (int): Number of parallel workers
    """
    logger.info(f"Starting image augmentation from {input_dir} to {output_dir}...")
    
    # Ensure input directory exists
    if not os.path.exists(input_dir):
        logger.error(f"Input directory not found: {input_dir}")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all query directories
    query_dirs = [d for d in os.listdir(input_dir) 
                 if os.path.isdir(os.path.join(input_dir, d))]
    
    logger.info(f"Found {len(query_dirs)} product categories")
    
    total_original = 0
    total_augmented = 0
    
    for query_dir in query_dirs:
        logger.info(f"Processing category: {query_dir}")
        
        # Create output directory for this query
        query_out_dir = os.path.join(output_dir, query_dir)
        os.makedirs(query_out_dir, exist_ok=True)
        
        # Get all images
        query_in_dir = os.path.join(input_dir, query_dir)
        image_files = glob.glob(os.path.join(query_in_dir, "*.jpg")) + \
                      glob.glob(os.path.join(query_in_dir, "*.png")) + \
                      glob.glob(os.path.join(query_in_dir, "*.jpeg"))
        
        if not image_files:
            logger.warning(f"No images found in {query_in_dir}")
            continue
        
        logger.info(f"Found {len(image_files)} images for {query_dir}")
        total_original += len(image_files)
        
        # Prepare arguments for parallel processing
        process_args = [(img_path, query_out_dir) for img_path in image_files]
        
        # Process images in parallel
        augmented_count = 0
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            for n_augmented in tqdm(executor.map(process_image, process_args), 
                                    total=len(process_args), 
                                    desc=query_dir):
                augmented_count += n_augmented
        
        logger.info(f"Created {augmented_count} augmented images for {query_dir}")
        total_augmented += augmented_count
    
    logger.info(f"Augmentation complete!")
    logger.info(f"Total original images: {total_original}")
    logger.info(f"Total augmented images: {total_augmented}")
    logger.info(f"Final dataset size: {total_original + total_augmented} images")
    
    return {
        "original": total_original,
        "augmented": total_augmented,
        "total": total_original + total_augmented
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Augment product images")
    parser.add_argument("--input-dir", default="data/images", help="Input directory with original images")
    parser.add_argument("--output-dir", default="data/augmented", help="Output directory for augmented images")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    
    args = parser.parse_args()
    
    results = process_all_images(args.input_dir, args.output_dir, args.workers)