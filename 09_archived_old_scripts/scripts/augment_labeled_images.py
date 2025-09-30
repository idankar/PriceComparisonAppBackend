import os
import cv2
import numpy as np
import random
import json
from tqdm import tqdm
import logging
import argparse
import glob
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

# Assuming config is accessible or we define paths here
# import config # If config.py is appropriately set up

# Define paths relative to script or use absolute paths/config
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
LABELED_DATA_FILE = PROJECT_ROOT / "data/labeled/labeled_data.json" # Assumed input
RAW_IMAGE_DIR = PROJECT_ROOT / "data/raw_images"
AUGMENTED_IMAGE_DIR = PROJECT_ROOT / "data/augmented"
AUGMENTED_METADATA_FILE = AUGMENTED_IMAGE_DIR / "augmented_metadata.json"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "augmentation.log"), # Log in project root
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def augment_image(img):
    """
    Create variants of the image with different transformations.
    (Copied from original image_augmentation.py)
    Returns a list of tuples: (augmentation_type, augmented_image_data)
    """
    augmented = []

    # Rotate slightly
    for angle in [-3, 3, -5, 5]: # Added more angles
        h, w = img.shape[:2]
        M = cv2.getRotationMatrix2D((w/2, h/2), angle, 1)
        rotated = cv2.warpAffine(img, M, (w, h), borderMode=cv2.BORDER_REPLICATE)
        augmented.append((f"rotate_{angle}", rotated))

    # Change brightness/contrast
    for alpha, beta in [(1.2, 10), (0.8, -10), (1.0, 30), (1.0, -30)]:
        adjusted = cv2.convertScaleAbs(img, alpha=alpha, beta=beta)
        augmented.append((f"bright_cont_{alpha}_{beta}", adjusted))

    # Add slight blur (Gaussian)
    for ksize in [(3,3), (5,5)]:
        blurred = cv2.GaussianBlur(img, ksize, 0)
        augmented.append((f"blur_{ksize[0]}", blurred))

    # Add noise
    noise = np.copy(img)
    noise_factor = random.uniform(0.03, 0.08) # Randomized noise factor
    noise_type = random.choice(['gauss', 's&p'])
    if noise_type == 'gauss':
        gauss_noise = np.random.normal(0, noise_factor * 255, noise.shape).astype(np.float32)
        noisy_img_float = cv2.add(noise.astype(np.float32), gauss_noise)
    else: # salt & pepper
        s_vs_p = 0.5
        amount = noise_factor * 0.1 # Smaller amount for s&p
        noisy_img_float = noise.astype(np.float32)
        # Salt
        num_salt = np.ceil(amount * noise.size * s_vs_p)
        coords = [np.random.randint(0, i - 1, int(num_salt)) for i in noise.shape]
        noisy_img_float[coords[0], coords[1], :] = 255
        # Pepper
        num_pepper = np.ceil(amount* noise.size * (1. - s_vs_p))
        coords = [np.random.randint(0, i - 1, int(num_pepper)) for i in noise.shape]
        noisy_img_float[coords[0], coords[1], :] = 0

    noise = np.clip(noisy_img_float, 0, 255).astype(np.uint8)
    augmented.append((f"noise_{noise_type}", noise))

    # Simple crop and resize (Simulates slight framing changes)
    h, w = img.shape[:2]
    for crop_percent in [0.05, 0.1]:
        crop_h, crop_w = int(h * crop_percent), int(w * crop_percent)
        if h - 2 * crop_h > 10 and w - 2 * crop_w > 10: # Ensure crop is valid
             cropped = img[crop_h:h-crop_h, crop_w:w-crop_w]
             # Resize back to original dimensions
             cropped_resized = cv2.resize(cropped, (w, h), interpolation=cv2.INTER_AREA)
             augmented.append((f"crop_{int(crop_percent*100)}", cropped_resized))

    return augmented

def process_single_labeled_item(item):
    """
    Processes one item from the labeled data (image + label).
    Applies augmentations and returns metadata for augmented images.
    """
    original_image_relative_path = item.get("image_path") # Path relative to RAW_IMAGE_DIR
    label = item.get("label")

    if not original_image_relative_path or not label:
        logger.warning(f"Skipping item due to missing image path or label: {item}")
        return []

    original_image_full_path = RAW_IMAGE_DIR / original_image_relative_path
    if not original_image_full_path.exists():
        logger.warning(f"Original image not found: {original_image_full_path}")
        return []

    try:
        img = cv2.imread(str(original_image_full_path))
        if img is None:
            logger.warning(f"Could not read image: {original_image_full_path}")
            return []

        base_name, ext = os.path.splitext(original_image_relative_path)
        augmented_metadata_list = []

        # Generate and save augmented images
        augmented_variants = augment_image(img)
        for aug_type, aug_img_data in augmented_variants:
            aug_filename = f"{Path(base_name).name}_{aug_type}{ext}"
            aug_save_path = AUGMENTED_IMAGE_DIR / aug_filename
            success = cv2.imwrite(str(aug_save_path), aug_img_data)
            if success:
                augmented_metadata_list.append({
                    "image_path": aug_filename, # Path relative to AUGMENTED_IMAGE_DIR
                    "label": label,
                    "augmentation_type": aug_type,
                    "original_image": original_image_relative_path
                })
            else:
                 logger.warning(f"Failed to write augmented image: {aug_save_path}")


        return augmented_metadata_list

    except Exception as e:
        logger.error(f"Error processing {original_image_full_path}: {str(e)}")
        return []

def run_augmentation(labeled_data_path, raw_img_dir, aug_img_dir, aug_meta_path, num_workers=4):
    """
    Main function to load labeled data, augment images, and save results.
    """
    logger.info(f"Starting image augmentation for labeled data: {labeled_data_path}")
    logger.info(f"Raw images expected in: {raw_img_dir}")
    logger.info(f"Augmented images will be saved to: {aug_img_dir}")

    if not Path(labeled_data_path).exists():
        logger.error(f"Labeled data file not found: {labeled_data_path}")
        return

    # Load labeled data
    try:
        with open(labeled_data_path, 'r', encoding='utf-8') as f:
            labeled_data = json.load(f)
        logger.info(f"Loaded {len(labeled_data)} labeled items.")
    except Exception as e:
        logger.error(f"Failed to load or parse JSON from {labeled_data_path}: {e}")
        return

    # Ensure output directory exists
    Path(aug_img_dir).mkdir(parents=True, exist_ok=True)

    all_augmented_metadata = []
    total_items = len(labeled_data)
    processed_count = 0
    failed_count = 0

    # Process images in parallel
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Map items to the processing function
        futures = [executor.submit(process_single_labeled_item, item) for item in labeled_data]
        for future in tqdm(futures, total=total_items, desc="Augmenting images"):
            try:
                result_metadata_list = future.result()
                if result_metadata_list: # If processing was successful and returned data
                    all_augmented_metadata.extend(result_metadata_list)
                    processed_count += 1
                else: # If processing failed or skipped
                     failed_count +=1 # Assuming failure if empty list returned, adjust if needed
            except Exception as e:
                logger.error(f"A worker process failed: {e}")
                failed_count += 1

    logger.info(f"Augmentation complete.")
    logger.info(f"Successfully processed items (generating augmentations): {processed_count}")
    logger.info(f"Failed/skipped items: {failed_count}")
    logger.info(f"Total augmented images generated: {len(all_augmented_metadata)}")

    # Save the metadata for augmented images
    try:
        with open(aug_meta_path, 'w', encoding='utf-8') as f:
            json.dump(all_augmented_metadata, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved metadata for augmented images to: {aug_meta_path}")
    except Exception as e:
        logger.error(f"Failed to save augmented metadata JSON: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Augment labeled images")
    parser.add_argument("--labeled-file", default=str(LABELED_DATA_FILE), help="Path to the JSON file containing labeled data.")
    parser.add_argument("--raw-dir", default=str(RAW_IMAGE_DIR), help="Directory containing the original raw images.")
    parser.add_argument("--aug-dir", default=str(AUGMENTED_IMAGE_DIR), help="Output directory for augmented images.")
    parser.add_argument("--meta-out", default=str(AUGMENTED_METADATA_FILE), help="Output JSON file for augmented image metadata.")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers.")

    args = parser.parse_args()

    run_augmentation(
        labeled_data_path=args.labeled_file,
        raw_img_dir=args.raw_dir,
        aug_img_dir=args.aug_dir,
        aug_meta_path=args.meta_out,
        num_workers=args.workers
    )
