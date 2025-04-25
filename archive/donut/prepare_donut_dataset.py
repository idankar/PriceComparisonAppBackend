# prepare_donut_dataset.py
import os
import json
import shutil
import logging
import csv
import random
from pathlib import Path
import argparse
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("donut_preparation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def prepare_dataset(results_dir, images_dir, augmented_dir, output_dir, train_ratio=0.8, include_augmented=True):
    """
    Prepare dataset for Donut training
    
    Args:
        results_dir (str): Directory containing CSV/JSON results
        images_dir (str): Directory containing original images
        augmented_dir (str): Directory containing augmented images
        output_dir (str): Output directory for Donut dataset
        train_ratio (float): Ratio of training vs validation split
        include_augmented (bool): Whether to include augmented images
    """
    logger.info("Preparing Donut dataset...")
    
    # Create output directories
    train_dir = os.path.join(output_dir, "train")
    val_dir = os.path.join(output_dir, "val")
    train_img_dir = os.path.join(train_dir, "images")
    val_img_dir = os.path.join(val_dir, "images")
    
    os.makedirs(train_dir, exist_ok=True)
    os.makedirs(val_dir, exist_ok=True)
    os.makedirs(train_img_dir, exist_ok=True)
    os.makedirs(val_img_dir, exist_ok=True)
    
    # Find all result directories
    query_results = {}
    
    for query_dir in os.listdir(results_dir):
        query_path = os.path.join(results_dir, query_dir)
        if os.path.isdir(query_path):
            # Find the JSON files in this directory
            json_files = [f for f in os.listdir(query_path) if f.endswith('.json')]
            
            if json_files:
                # Use the most recent JSON file
                latest_json = max(json_files, key=lambda x: os.path.getmtime(os.path.join(query_path, x)))
                query_results[query_dir] = os.path.join(query_path, latest_json)
    
    logger.info(f"Found result files for {len(query_results)} product categories")
    
    # Process each result file and prepare data
    train_data = []
    val_data = []
    total_products = 0
    skipped_products = 0
    
    for query_dir, json_file in tqdm(query_results.items(), desc="Processing categories"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                products = json.load(f)
            
            logger.info(f"Processing {len(products)} products from {query_dir}")
            
            # Process each product
            for product in products:
                try:
                    # Check if image exists
                    image_path = product.get('image_path')
                    if not image_path or not os.path.exists(image_path):
                        skipped_products += 1
                        continue
                    
                    # Prepare label in Donut format
                    label = {
                        "product_name": product.get("product_name", ""),
                        "price": product.get("price", ""),
                        "formatted_price": product.get("formatted_price", ""),
                        "brand": product.get("brand", ""),
                        "query": product.get("query", ""),
                        "unit_description": product.get("unit_description", "")
                    }
                    
                    # Create example
                    example = {
                        "image_path": image_path,
                        "gt_parse": json.dumps(label, ensure_ascii=False)
                    }
                    
                    # Randomly assign to train or val set
                    if random.random() < train_ratio:
                        train_data.append(example)
                    else:
                        val_data.append(example)
                    
                    total_products += 1
                    
                    # If including augmented images, add them too
                    if include_augmented:
                        # Get the base image filename
                        image_basename = os.path.basename(image_path)
                        name, ext = os.path.splitext(image_basename)
                        
                        # Look for augmented versions
                        query_aug_dir = os.path.join(augmented_dir, query_dir)
                        if os.path.exists(query_aug_dir):
                            aug_pattern = f"{name}_*{ext}"
                            aug_files = Path(query_aug_dir).glob(aug_pattern)
                            
                            for aug_file in aug_files:
                                aug_example = {
                                    "image_path": str(aug_file),
                                    "gt_parse": json.dumps(label, ensure_ascii=False)
                                }
                                
                                # Add to the same set as the original
                                if example in train_data:
                                    train_data.append(aug_example)
                                else:
                                    val_data.append(aug_example)
                                
                                total_products += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing product: {str(e)}")
                    skipped_products += 1
        
        except Exception as e:
            logger.error(f"Error processing {query_dir}: {str(e)}")
    
    logger.info(f"Processed {total_products} products, skipped {skipped_products}")
    logger.info(f"Training examples: {len(train_data)}")
    logger.info(f"Validation examples: {len(val_data)}")
    
    # Save datasets
    train_json = os.path.join(train_dir, "metadata.jsonl")
    val_json = os.path.join(val_dir, "metadata.jsonl")
    
    with open(train_json, 'w', encoding='utf-8') as f:
        for example in train_data:
            # Copy image to training directory
            src_path = example["image_path"]
            dst_filename = f"{os.path.basename(os.path.dirname(src_path))}_{os.path.basename(src_path)}"
            dst_path = os.path.join(train_img_dir, dst_filename)
            shutil.copy2(src_path, dst_path)
            
            # Update path in example
            example["image_path"] = f"images/{dst_filename}"
            
            # Write as JSON line
            f.write(json.dumps(example, ensure_ascii=False) + '\n')
    
    with open(val_json, 'w', encoding='utf-8') as f:
        for example in val_data:
            # Copy image to validation directory
            src_path = example["image_path"]
            dst_filename = f"{os.path.basename(os.path.dirname(src_path))}_{os.path.basename(src_path)}"
            dst_path = os.path.join(val_img_dir, dst_filename)
            shutil.copy2(src_path, dst_path)
            
            # Update path in example
            example["image_path"] = f"images/{dst_filename}"
            
            # Write as JSON line
            f.write(json.dumps(example, ensure_ascii=False) + '\n')
    
    logger.info(f"Saved training metadata to {train_json}")
    logger.info(f"Saved validation metadata to {val_json}")
    logger.info("Dataset preparation complete!")
    
    return {
        "train_examples": len(train_data),
        "val_examples": len(val_data),
        "total_examples": total_products
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare dataset for Donut training")
    parser.add_argument("--results-dir", default="data/results", help="Directory containing result files")
    parser.add_argument("--images-dir", default="data/images", help="Directory containing original images")
    parser.add_argument("--augmented-dir", default="data/augmented", help="Directory containing augmented images")
    parser.add_argument("--output-dir", default="data/donut", help="Output directory for Donut dataset")
    parser.add_argument("--train-ratio", type=float, default=0.8, help="Ratio of training vs validation split")
    parser.add_argument("--include-augmented", action="store_true", help="Include augmented images")
    
    args = parser.parse_args()
    
    results = prepare_dataset(
        args.results_dir, 
        args.images_dir, 
        args.augmented_dir, 
        args.output_dir,
        args.train_ratio,
        args.include_augmented
    )