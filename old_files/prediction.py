#!/usr/bin/env python3
# predict.py - Donut model inference script

import os
import sys
import json
import logging
import argparse
from PIL import Image
import torch
from transformers import DonutProcessor, VisionEncoderDecoderModel

import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "predict.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def predict_image(image_path, model_dir=None, prompt=None):
    """
    Run inference on a single image using the Donut model
    
    Args:
        image_path (str): Path to the image
        model_dir (str): Path to the model directory
        prompt (str): Prompt for structured extraction
        
    Returns:
        dict: Extracted information
    """
    # Use default model or provided model
    if model_dir is None:
        processor = DonutProcessor.from_pretrained(config.DONUT_MODEL)
        model = VisionEncoderDecoderModel.from_pretrained(config.DONUT_MODEL)
    else:
        processor = DonutProcessor.from_pretrained(model_dir)
        model = VisionEncoderDecoderModel.from_pretrained(model_dir)
    
    # Set device
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    
    # Set model to eval mode
    model.eval()
    
    # Use default prompt if none provided
    if prompt is None:
        prompt = "Extract product name and price in Hebrew.\nOutput format: {'name': ..., 'price': ...}"
    
    # Load and process image
    image = Image.open(image_path).convert("RGB")
    pixel_values = processor(image, return_tensors="pt").pixel_values.to(device)
    
    # Create decoder input from prompt
    decoder_input_ids = processor.tokenizer(
        prompt,
        add_special_tokens=False,
        return_tensors="pt"
    ).input_ids.to(device)
    
    # Generate prediction
    outputs = model.generate(
        pixel_values,
        decoder_input_ids=decoder_input_ids,
        max_length=128,
        early_stopping=True,
        pad_token_id=processor.tokenizer.pad_token_id
    )
    
    # Decode the prediction
    prediction = processor.batch_decode(outputs, skip_special_tokens=True)[0]
    
    # Try to parse as JSON
    try:
        result = json.loads(prediction)
        return result
    except json.JSONDecodeError:
        logger.warning(f"Could not parse prediction as JSON: {prediction}")
        return {"raw_output": prediction}

def main():
    parser = argparse.ArgumentParser(description="Run inference with Donut model")
    parser.add_argument("image_path", help="Path to the image")
    parser.add_argument("--model_dir", default=None, help="Path to the model directory")
    parser.add_argument("--prompt", default=None, help="Prompt for structured extraction")
    args = parser.parse_args()
    
    if not os.path.exists(args.image_path):
        logger.error(f"Image not found: {args.image_path}")
        sys.exit(1)
    
    result = predict_image(
        image_path=args.image_path,
        model_dir=args.model_dir,
        prompt=args.prompt
    )
    
    # Print result
    print(json.dumps(result, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()