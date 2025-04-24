# gpt4_vision_labeler.py
import os
import json
import base64
import argparse
import logging
import time
import random
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

# Rate limiting settings
RATE_LIMIT_DELAY = 1          # seconds between API calls
INITIAL_RETRY_DELAY = 5       # start with 5 seconds
MAX_RETRY_DELAY = 60          # max 60 seconds
RATE_LIMIT_BATCH_SIZE = 10    # process in small batches

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
    
    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    response.raise_for_status()
    
    response_data = response.json()
    if "choices" in response_data and len(response_data["choices"]) > 0:
        result = json.loads(response_data["choices"][0]["message"]["content"])
        return result
    else:
        logger.warning(f"Unexpected response format for {image_path}")
        return None

def analyze_image_with_backoff(image_path, api_key):
    """Analyze image with exponential backoff for rate limiting"""
    retries = 5
    delay = INITIAL_RETRY_DELAY
    for attempt in range(retries):
        try:
            return analyze_image(image_path, api_key)
        except requests.exceptions.RequestException as e:
            if "429" in str(e) and attempt < retries - 1:
                logger.warning(f"Rate limited (attempt {attempt+1}): {e}. Retrying in {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
            elif attempt < retries - 1:
                logger.warning(f"Request failed (attempt {attempt+1}): {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"Failed after {retries} attempts: {e}")
                return None
    return None

def process_images(image_dir, output_file, api_key, max_images=None, workers=1):
    """Process all images in a directory with rate limiting and batch saving"""
    logger.info(f"Processing images from {image_dir}")

    # Ensure output dir exists
    out_dir = os.path.dirname(output_file)
    os.makedirs(out_dir, exist_ok=True)

    # Gather images
    paths = []
    for root, _, files in os.walk(image_dir):
        for f in files:
            if f.lower().endswith(('.png','.jpg','.jpeg')):
                paths.append(os.path.join(root, f))
    if max_images and len(paths) > max_images:
        random.shuffle(paths)
        paths = paths[:max_images]
    logger.info(f"Found {len(paths)} images, processing {len(paths)} (max_images={max_images})")

    results = []
    processed = 0
    with tqdm(total=len(paths), desc="Labeling Images") as pbar:
        for i in range(0, len(paths), RATE_LIMIT_BATCH_SIZE):
            batch = paths[i:i+RATE_LIMIT_BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=workers) as executor:
                for img in batch:
                    time.sleep(RATE_LIMIT_DELAY)
                    res = analyze_image_with_backoff(img, api_key)
                    if res:
                        pid = os.path.splitext(os.path.basename(img))[0]
                        results.append({"product_id": pid, "image_path": img, **res})
                    processed += 1
                    pbar.update(1)
            # Save intermediate results
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info(f"Processed {processed} images, got {len(results)} results")
    logger.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Label product images using GPT-4o Vision API")
    parser.add_argument("--image-dir", required=True, help="Directory containing product images")
    parser.add_argument("--output-file", default=str(DEFAULT_OUTPUT_DIR / "labeled_data.json"), help="Output JSON file for combined labeled data")
    parser.add_argument("--api-key", required=True, help="OpenAI API key")
    parser.add_argument("--max-images", type=int, default=None, help="Maximum number of images to process")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker threads")
    
    args = parser.parse_args()
    
    process_images(
        args.image_dir,
        args.output_file,
        args.api_key,
        args.max_images,
        args.workers
    )