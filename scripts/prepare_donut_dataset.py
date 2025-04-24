import os
import json
import random
import logging
import argparse
from pathlib import Path
from sklearn.model_selection import train_test_split

# Assuming config is accessible or we define paths here
# import config # If config.py is appropriately set up

# Define paths relative to script or use absolute paths/config
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
AUGMENTED_METADATA_FILE = PROJECT_ROOT / "data/augmented/augmented_metadata.json" # Assumed input
AUGMENTED_IMAGE_DIR = PROJECT_ROOT / "data/augmented"
DONUT_OUTPUT_DIR = PROJECT_ROOT / "data/donut"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "prepare_dataset.log"), # Log in project root
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def format_ground_truth(label_dict):
    """Convert a label dictionary into the Donut required JSON string format."""
    # Example: Convert {"product_name": "Nutella", "price": "15.90"} 
    # to the string '{"gt_parse": {"product_name": "Nutella", "price": "15.90"}}'
    # Adjust this formatting based on the exact requirements of your Donut model/processor
    return json.dumps({"gt_parse": label_dict}, ensure_ascii=False)

def create_donut_metadata(augmented_metadata_path, augmented_img_dir, output_dir, test_size=0.1):
    """
    Reads augmented metadata, formats labels for Donut, splits into train/val,
    and saves metadata files (e.g., metadata.jsonl) in the output directory.
    """
    logger.info(f"Preparing Donut dataset from: {augmented_metadata_path}")
    logger.info(f"Augmented images expected in: {augmented_img_dir}")
    logger.info(f"Donut metadata will be saved to: {output_dir}")

    if not Path(augmented_metadata_path).exists():
        logger.error(f"Augmented metadata file not found: {augmented_metadata_path}")
        return

    try:
        with open(augmented_metadata_path, 'r', encoding='utf-8') as f:
            augmented_data = json.load(f)
        logger.info(f"Loaded {len(augmented_data)} augmented items.")
    except Exception as e:
        logger.error(f"Failed to load or parse JSON from {augmented_metadata_path}: {e}")
        return

    # Ensure output directories exist
    train_dir = Path(output_dir) / "train"
    val_dir = Path(output_dir) / "val"
    train_dir.mkdir(parents=True, exist_ok=True)
    val_dir.mkdir(parents=True, exist_ok=True)

    processed_examples = []
    missing_images = 0

    for item in tqdm(augmented_data, desc="Processing augmented items"):
        image_relative_path = item.get("image_path") # Path relative to augmented_img_dir
        label_dict = item.get("label")

        if not image_relative_path or not label_dict:
            logger.warning("Skipping item with missing image path or label.")
            continue

        image_full_path = Path(augmented_img_dir) / image_relative_path
        if not image_full_path.exists():
            logger.warning(f"Augmented image file missing: {image_full_path}")
            missing_images += 1
            continue

        # Format the ground truth string
        ground_truth_str = format_ground_truth(label_dict)

        processed_examples.append({
            # Store path relative to the train/val metadata.jsonl file
            "file_name": image_relative_path, 
            "ground_truth": ground_truth_str
        })

    if missing_images > 0:
        logger.warning(f"Could not find {missing_images} augmented images.")

    logger.info(f"Successfully processed {len(processed_examples)} items for Donut format.")

    # Split into training and validation sets
    if not processed_examples:
         logger.error("No valid examples to split.")
         return
         
    try:
        train_data, val_data = train_test_split(processed_examples, test_size=test_size, random_state=42)
        logger.info(f"Split data into {len(train_data)} training and {len(val_data)} validation examples.")
    except ValueError as e:
        logger.error(f"Error splitting data (maybe too few samples?): {e}")
        # Handle case with very few samples - put all in training?
        if len(processed_examples) < 5:
             logger.warning("Too few samples, putting all in training set.")
             train_data = processed_examples
             val_data = []
        else:
             return # Or raise error

    # Save metadata files (JSON Lines format is common)
    train_meta_path = train_dir / "metadata.jsonl"
    val_meta_path = val_dir / "metadata.jsonl"

    try:
        with open(train_meta_path, 'w', encoding='utf-8') as f_train:
            for example in train_data:
                # Adjust image path to be relative to the metadata file location
                # Assuming images are directly in augmented_img_dir for simplicity now
                # We might need to copy/link images into train/val subdirs later depending on the Trainer
                f_train.write(json.dumps(example, ensure_ascii=False) + '\n')
        logger.info(f"Saved training metadata to {train_meta_path}")

        with open(val_meta_path, 'w', encoding='utf-8') as f_val:
            for example in val_data:
                f_val.write(json.dumps(example, ensure_ascii=False) + '\n')
        logger.info(f"Saved validation metadata to {val_meta_path}")

    except Exception as e:
        logger.error(f"Failed to write metadata files: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare Donut dataset from augmented data")
    parser.add_argument("--aug-meta-file", default=str(AUGMENTED_METADATA_FILE), help="Path to the augmented metadata JSON file.")
    parser.add_argument("--aug-img-dir", default=str(AUGMENTED_IMAGE_DIR), help="Directory containing the augmented images.")
    parser.add_argument("--output-dir", default=str(DONUT_OUTPUT_DIR), help="Output directory for Donut train/val metadata.")
    parser.add_argument("--test-size", type=float, default=0.1, help="Proportion of data to use for validation set.")

    args = parser.parse_args()

    create_donut_metadata(
        augmented_metadata_path=args.aug_meta_file,
        augmented_img_dir=args.aug_img_dir,
        output_dir=args.output_dir,
        test_size=args.test_size
    )
