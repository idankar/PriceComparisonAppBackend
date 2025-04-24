# gpt4_vision_labeler.py
import os
import json
import base64
import argparse
import logging
import time
from pathlib import Path
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vision_labeling.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def encode_image(image_path):
    """Encode image to base64"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def analyze_image(image_path, api_key):
    """Analyze an image with GPT-4o Vision API"""
    base64_image = encode_image(image_path)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "system",
                "content": "You are a precise product information extractor. Analyze the grocery product in the image and return ONLY a JSON object with these fields: product_name, brand, amount, unit, category. If you can't determine a field, use null. Do not include any explanations, just the JSON."
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What product is this? Extract the exact product name, brand, amount, and unit."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }
    
    max_retries = 3
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            response.raise_for_status()
            
            response_data = response.json()
            if "choices" in response_data and len(response_data["choices"]) > 0:
                result = json.loads(response_data["choices"][0]["message"]["content"])
                return result
            else:
                logger.warning(f"Unexpected response format for {image_path}")
                return None
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Request failed (attempt {attempt+1}): {str(e)}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to analyze {image_path} after {max_retries} attempts: {str(e)}")
                return None
        except json.JSONDecodeError:
            logger.error(f"Failed to parse response as JSON for {image_path}")
            return None
    
    return None

def process_image(args):
    """Process single image with retry mechanism"""
    image_path, api_key, output_dir = args
    
    product_id = os.path.splitext(os.path.basename(image_path))[0]
    output_file = os.path.join(output_dir, f"{product_id}.json")
    
    # Skip if output already exists
    if os.path.exists(output_file):
        return None
    
    # Analyze the image
    result = analyze_image(image_path, api_key)
    
    if result:
        # Add image path to result
        result["image_path"] = str(Path(image_path).relative_to(Path(args[0]).parent)) # Store relative path
        result["product_id"] = product_id
        
        # Save result to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        return result
    
    return None

def process_images(images_dir, output_dir, api_key, max_images=None, workers=4):
    """Process all images in a directory, up to max_images."""
    logger.info(f"Processing images from {images_dir}")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Find all images
    image_paths_full = list(Path(images_dir).rglob('*.png')) + \
                       list(Path(images_dir).rglob('*.jpg')) + \
                       list(Path(images_dir).rglob('*.jpeg'))
    
    # Shuffle and select max_images if specified
    random.shuffle(image_paths_full)
    if max_images is not None and max_images > 0:
        image_paths = image_paths_full[:max_images]
        logger.info(f"Found {len(image_paths_full)} images, processing {len(image_paths)} (max_images={max_images})")
    else:
        image_paths = image_paths_full
        logger.info(f"Found {len(image_paths)} images to process")
    
    # Prepare arguments for parallel processing
    task_args = [(str(path), api_key, output_dir) for path in image_paths]
    results = []
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Use tqdm for progress bar
        for result in tqdm(executor.map(lambda p: process_image(p), task_args), total=len(task_args), desc="Labeling Images"):
            if result:
                # Store image path relative to the output_dir's parent for consistency?
                # Or relative to the project root?
                # Current process_image saves absolute path, let's adjust that.
                # Re-adjust path to be relative to the project root for the combined JSON
                try:
                     result["image_path"] = str(Path(result["image_path"]).relative_to(PROJECT_ROOT))
                except ValueError:
                     pass # Keep absolute if not within project root (shouldn't happen here)
                results.append(result)
    
    # Save all successful results to a single JSON file
    if results:
        all_results_file = Path(output_dir) / "labeled_data.json" # Consistent name
        try:
            with open(all_results_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {len(results)} images successfully")
            logger.info(f"Combined labeling results saved to {all_results_file}")
        except Exception as e:
             logger.error(f"Failed to save combined JSON {all_results_file}: {e}")
    else:
        logger.warning("No images were processed successfully")

if __name__ == "__main__":
    # Define project root based on script location
    SCRIPT_DIR = Path(__file__).parent.resolve()
    PROJECT_ROOT = SCRIPT_DIR.parent
    DEFAULT_IMAGES_DIR = PROJECT_ROOT / "data/raw_images"
    DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/labeled"
    
    parser = argparse.ArgumentParser(description="Label product images using GPT-4o Vision API")
    parser.add_argument("--image-dir", default=str(DEFAULT_IMAGES_DIR), help="Directory containing product images")
    parser.add_argument("--output-file", default=str(DEFAULT_OUTPUT_DIR / "labeled_data.json"), help="Output JSON file for combined labeled data")
    # API Key: Better to use environment variable
    parser.add_argument("--api-key", default=os.environ.get("OPENAI_API_KEY"), help="OpenAI API key (or set OPENAI_API_KEY env var)")
    parser.add_argument("--max-images", type=int, default=None, help="Maximum number of images to process (default: all)")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    
    args = parser.parse_args()
    
    if not args.api_key:
        logger.error("OpenAI API key is required. Set --api-key or the OPENAI_API_KEY environment variable.")
        sys.exit(1)
        
    # The output directory for individual JSONs is the parent of the combined file
    output_dir_individual_jsons = Path(args.output_file).parent

    process_images(
        images_dir=args.image_dir, 
        output_dir=str(output_dir_individual_jsons), # Dir for individual files
        api_key=args.api_key, 
        max_images=args.max_images, 
        workers=args.workers
    )
