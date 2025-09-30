# scripts/single_image_analysis.py
import os
import json
import base64
import argparse
import logging
import time
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("single_image.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def encode_image(image_path):
    """Encode image to base64"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def analyze_single_image(image_path, api_key):
    """Analyze a single image with GPT-4o Vision API"""
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
    
    retry_delay = 30  # Start with 30 seconds
    max_retries = 12  # Try up to 12 times (6 minutes total)
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Making API request (attempt {attempt+1}/{max_retries})...")
            response = requests.post(
                "https://api.openai.com/v1/chat/completions", 
                headers=headers, 
                json=payload, 
                timeout=60
            )
            
            if response.status_code == 200:
                logger.info("Request successful!")
                response_data = response.json()
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    result = json.loads(response_data["choices"][0]["message"]["content"])
                    return result
                else:
                    logger.warning(f"Unexpected response format")
                    print(f"Response data: {response_data}")
            elif response.status_code == 429:
                logger.warning(f"Rate limited. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 120)  # Increase delay, cap at 2 minutes
                continue
            else:
                logger.error(f"Request failed with status {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error during request: {str(e)}")
            
        # Sleep before retry
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 1.5, 120)
    
    logger.error(f"Failed to analyze {image_path} after {max_retries} attempts")
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze a single product image with GPT-4o Vision")
    parser.add_argument("--image-path", required=True, help="Path to image file")
    parser.add_argument("--api-key", required=True, help="OpenAI API key")
    parser.add_argument("--output-file", help="Output JSON file", default="single_image_result.json")
    
    args = parser.parse_args()
    
    result = analyze_single_image(args.image_path, args.api_key)
    
    if result:
        # Add image path to result
        result["image_path"] = args.image_path
        result["product_id"] = os.path.splitext(os.path.basename(args.image_path))[0]
        
        # Print result
        print("\nAnalysis Result:")
        print(f"Product: {result.get('product_name', 'Unknown')}")
        print(f"Brand: {result.get('brand', 'Unknown')}")
        print(f"Amount: {result.get('amount', 'Unknown')} {result.get('unit', '')}")
        
        # Save result
        with open(args.output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\nResult saved to {args.output_file}")
    else:
        print("\nFailed to analyze image after multiple attempts.")