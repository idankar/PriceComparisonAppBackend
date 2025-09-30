# scripts/slow_gpt4_vision.py
import os
import json
import base64
import argparse
import logging
import time
import random
from tqdm import tqdm
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("slow_vision.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Very conservative rate limiting
REQUEST_DELAY = 20  # seconds between requests
MAX_RETRIES = 10
INITIAL_RETRY_DELAY = 20  # seconds
MAX_RETRY_DELAY = 120  # seconds

def encode_image(image_path):
    """Encode image to base64"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def analyze_image(image_path, api_key):
    """Analyze an image with GPT-4o Vision API using very conservative rate limiting"""
    logger.info(f"Analyzing image: {image_path}")
    
    # Encode image
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
                "content": (
                    "You are a precise product information extractor. Analyze the grocery product in the image and extract ONLY "
                    "information that is VISUALLY PRESENT in the image. Return a JSON object with these fields: "
                    "product_name, brand, amount, unit. If you can't see a field in the image, use null. "
                    "IMPORTANT: Only include information you can actually see in the image. Do not guess or infer information "
                    "that isn't visible. Do not include any explanations, just the JSON."
                )
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "What product information can you see in this image? Extract ONLY visually present details."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }
    
    retry_delay = INITIAL_RETRY_DELAY
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions", 
                headers=headers, 
                json=payload, 
                timeout=60  # Longer timeout
            )
            
            if response.status_code == 200:
                response_data = response.json()
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    result = json.loads(response_data["choices"][0]["message"]["content"])
                    return result
                else:
                    logger.warning(f"Unexpected response format for {image_path}")
            elif response.status_code == 429:
                # Rate limited - log and retry
                logger.warning(f"Rate limited (attempt {attempt+1}/{MAX_RETRIES}). Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                # Increase delay for next attempt (exponential backoff)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue
            else:
                logger.error(f"Request failed with status {response.status_code}: {response.text}")
                
        except Exception as e:
            logger.error(f"Error during request (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
            
        # Sleep before retry
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
    
    logger.error(f"Failed to analyze {image_path} after {MAX_RETRIES} attempts")
    return None

def process_images(image_dir, output_file, api_key, max_images=None):
    """Process images with very conservative rate limiting"""
    logger.info(f"Processing images from {image_dir}")
    
    # Create output directory
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all images
    image_paths = []
    for root, _, files in os.walk(image_dir):
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                image_paths.append(os.path.join(root, file))
    
    # Limit the number of images if specified
    if max_images and len(image_paths) > max_images:
        random.shuffle(image_paths)
        image_paths = image_paths[:max_images]
    
    logger.info(f"Found {len(image_paths)} images, processing {len(image_paths)}")
    
    results = []
    
    for i, image_path in enumerate(tqdm(image_paths, desc="Analyzing Images")):
        # Load existing results if any
        if os.path.exists(output_file):
            with open(output_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            # Skip images already processed
            already_processed = [r["image_path"] for r in results if "image_path" in r]
            if image_path in already_processed:
                logger.info(f"Skipping already processed image: {image_path}")
                continue
        
        # Add significant delay between requests
        if i > 0:
            logger.info(f"Waiting {REQUEST_DELAY} seconds before next request...")
            time.sleep(REQUEST_DELAY)
        
        # Process image
        result = analyze_image(image_path, api_key)
        
        if result:
            # Add image path to result
            result["image_path"] = image_path
            result["product_id"] = os.path.splitext(os.path.basename(image_path))[0]
            
            # Print result
            print(f"\nProduct: {result.get('product_name', 'Unknown')}")
            print(f"Brand: {result.get('brand', 'Unknown')}")
            print(f"Amount: {result.get('amount', 'Unknown')} {result.get('unit', '')}")
            print("-" * 50)
            
            results.append(result)
            
            # Save after each successful result
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Processed {len(results)} images successfully")
    logger.info(f"Results saved to {output_file}")
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze product images with GPT-4o Vision (slow)")
    parser.add_argument("--image-dir", required=True, help="Directory containing product images")
    parser.add_argument("--output-file", required=True, help="Output JSON file for results")
    parser.add_argument("--api-key", required=True, help="OpenAI API key")
    parser.add_argument("--max-images", type=int, default=None, help="Maximum number of images to process")
    
    args = parser.parse_args()
    
    process_images(args.image_dir, args.output_file, args.api_key, args.max_images)