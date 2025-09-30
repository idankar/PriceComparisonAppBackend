import os
import sys
import json
import logging
import argparse
from PIL import Image
import torch
from transformers import DonutProcessor, VisionEncoderDecoderModel
from pathlib import Path

# Define paths relative to script or use absolute paths/config
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_MODEL_DIR = PROJECT_ROOT / "models/donut_trained/final_model" # Default location

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "predict.log"), # Log in project root
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def predict_image(image_path, model_dir=None, prompt=None):
    """
    Run inference on a single image using the trained Donut model

    Args:
        image_path (str): Path to the image
        model_dir (str): Path to the directory containing the final model and processor
        prompt (str): Prompt for structured extraction (specific to Donut model)

    Returns:
        dict: Extracted information
    """
    # Use provided model dir or default
    model_load_path = Path(model_dir) if model_dir else DEFAULT_MODEL_DIR
    logger.info(f"Loading model from: {model_load_path}")

    if not model_load_path.exists():
        logger.error(f"Model directory not found: {model_load_path}")
        # Fallback or specific error handling needed
        # Maybe try loading a base HF model? For now, error out.
        raise FileNotFoundError(f"Model directory not found: {model_load_path}")

    try:
        processor = DonutProcessor.from_pretrained(str(model_load_path))
        model = VisionEncoderDecoderModel.from_pretrained(str(model_load_path))
    except Exception as e:
        logger.error(f"Failed to load model/processor from {model_load_path}: {e}")
        raise

    # Set device
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    logger.info(f"Using device: {device}")

    # Set model to eval mode
    model.eval()

    # Default prompt if none provided (Adjust based on how your model was trained)
    # This assumes the model expects a task prompt
    if prompt is None:
        # Example prompt structure - modify as needed!
        task_prompt = "<s_cord-v2>" # Example for cord-v2 base model
        # Or maybe based on your labels: "<s_product_extraction>"
        # prompt = "Extract product details (name, price, brand, unit). Format: {'name': ..., 'price': ..., ...}"
        logger.warning("No prompt provided, using default task token <s_cord-v2>. Adjust if needed!")
        prompt = task_prompt
    else:
         task_prompt = prompt # Use user-provided prompt

    # Load and process image
    try:
        image = Image.open(image_path).convert("RGB")
    except FileNotFoundError:
        logger.error(f"Input image not found: {image_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to open image {image_path}: {e}")
        raise

    # Prepare inputs for the model
    pixel_values = processor(image, return_tensors="pt").pixel_values.to(device)
    decoder_input_ids = processor.tokenizer(task_prompt, add_special_tokens=False, return_tensors="pt").input_ids.to(device)

    # Generate prediction
    logger.info("Running model inference...")
    try:
        with torch.no_grad(): # Ensure no gradients are calculated
             outputs = model.generate(
                 pixel_values,
                 decoder_input_ids=decoder_input_ids,
                 max_length=model.decoder.config.max_position_embeddings, # Use model's max length
                 early_stopping=True,
                 pad_token_id=processor.tokenizer.pad_token_id,
                 eos_token_id=processor.tokenizer.eos_token_id,
                 use_cache=True,
                 num_beams=1, # Use greedy search for simplicity
                 bad_words_ids=[[processor.tokenizer.unk_token_id]],
                 return_dict_in_generate=True,
             )
    except Exception as e:
        logger.error(f"Model generation failed: {e}")
        raise

    # Decode the prediction
    sequence = processor.batch_decode(outputs.sequences)[0]
    # Remove prompt and special tokens
    sequence = sequence.replace(processor.tokenizer.eos_token, "").replace(processor.tokenizer.pad_token, "")
    sequence = sequence.replace(task_prompt, "").strip()
    logger.info(f"Raw prediction sequence: {sequence}")

    # Try to parse the sequence as structured data (adjust parser based on expected format)
    try:
        # Example: If model outputs JSON directly in the sequence
        # Find the first { and last } to extract potential JSON
        start_idx = sequence.find('{')
        end_idx = sequence.rfind('}')
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            json_str = sequence[start_idx : end_idx+1]
            result = json.loads(json_str)
            logger.info(f"Parsed JSON result: {result}")
            return result
        else:
             logger.warning(f"Could not find valid JSON structure in prediction: {sequence}")
             return {"raw_output": sequence}
    except json.JSONDecodeError as e:
        logger.warning(f"Could not parse prediction as JSON: {sequence}. Error: {e}")
        return {"raw_output": sequence}
    except Exception as e:
        logger.error(f"Error parsing prediction: {e}")
        return {"raw_output": sequence} # Return raw output on other errors

def main():
    parser = argparse.ArgumentParser(description="Run inference with trained Donut model")
    parser.add_argument("image_path", help="Path to the input image")
    parser.add_argument("--model-dir", default=None, help=f"Path to the final model directory (default: {DEFAULT_MODEL_DIR})")
    parser.add_argument("--prompt", default=None, help="Optional task prompt (e.g., <s_cord-v2>)")
    args = parser.parse_args()

    try:
        result = predict_image(
            image_path=args.image_path,
            model_dir=args.model_dir,
            prompt=args.prompt
        )
        # Print result as JSON
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except FileNotFoundError as e:
         logger.error(f"File not found error: {e}")
         sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred during prediction: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
