# scripts/test_gpt4_vision.py
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("vision_test.log"),
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
    
    # Add delay to avoid rate limiting
    time.sleep(2)
    
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
        logger.error(f"Request failed: {str(e)}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Failed to parse response as JSON for {image_path}")
        return None

def test_on_sample(image_dir, output_file, api_key, sample_size=20):
    """Test GPT-4o Vision API on a small sample of images"""
    logger.info(f"Testing GPT-4o Vision on a sample of {sample_size} images from {image_dir}")
    
    # Create output directory
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all images
    image_paths = []
    for root, _, files in os.walk(image_dir):
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                image_paths.append(os.path.join(root, file))
    
    # Select random sample
    if len(image_paths) > sample_size:
        random.shuffle(image_paths)
        image_paths = image_paths[:sample_size]
    
    logger.info(f"Found {len(image_paths)} images, testing on {len(image_paths)}")
    
    # Process images
    results = []
    
    for image_path in tqdm(image_paths, desc="Processing Test Images"):
        product_id = os.path.splitext(os.path.basename(image_path))[0]
        
        # Get product data from API
        result = analyze_image(image_path, api_key)
        
        if result:
            # Add image info to result
            result["image_path"] = image_path
            result["product_id"] = product_id
            results.append(result)
            
            # Print the result for immediate feedback
            print(f"\nProduct: {result.get('product_name', 'Unknown')}")
            print(f"Brand: {result.get('brand', 'Unknown')}")
            print(f"Amount: {result.get('amount', 'Unknown')} {result.get('unit', '')}")
            print(f"Category: {result.get('category', 'Unknown')}")
            print("-" * 50)
        else:
            logger.error(f"Failed to analyze {image_path}")
    
    # Save all results to a single file
    if results:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Processed {len(results)} images successfully")
        logger.info(f"Results saved to {output_file}")
    else:
        logger.warning("No images were processed successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test GPT-4o Vision API on product images")
    parser.add_argument("--image-dir", required=True, help="Directory containing product images")
    parser.add_argument("--output-file", default="data/test_results.json", help="Output file for test results")
    parser.add_argument("--api-key", required=True, help="OpenAI API key")
    parser.add_argument("--sample-size", type=int, default=20, help="Number of images to test")
    
    args = parser.parse_args()
    
    test_on_sample(args.image_dir, args.output_file, args.api_key, args.sample_size)