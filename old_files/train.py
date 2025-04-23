#!/usr/bin/env python3
# train.py - Donut model training script

import os
import sys
import json
import logging
import argparse
from PIL import Image
from torch.utils.data import Dataset
import torch
from transformers import DonutProcessor, VisionEncoderDecoderModel, Seq2SeqTrainer, Seq2SeqTrainingArguments

import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "train.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DonutDataset(Dataset):
    def __init__(self, examples, processor):
        self.examples = []
        self.processor = processor
        
        for entry in examples:
            image_path = os.path.join(config.DONUT_IMAGES_DIR, entry["image"])
            
            if not os.path.exists(image_path):
                logger.warning(f"Image not found: {image_path}")
                continue
                
            try:
                image = Image.open(image_path).convert("RGB")
                pixel_values = processor(image, return_tensors="pt").pixel_values[0]

                target_str = json.dumps(entry["label"], ensure_ascii=False)
                decoder_input_ids = processor.tokenizer(
                    target_str,
                    add_special_tokens=False,
                    max_length=128,
                    truncation=True,
                    return_tensors="pt"
                ).input_ids[0]

                self.examples.append({
                    "pixel_values": pixel_values.clone().detach(),
                    "labels": decoder_input_ids.clone().detach()
                })
                
                if len(self.examples) % 10 == 0:
                    logger.info(f"Loaded {len(self.examples)} examples")
                    
            except Exception as e:
                logger.error(f"Error processing {image_path}: {str(e)}")
                continue

    def __len__(self):
        return len(self.examples)

    def __getitem__(self, idx):
        return self.examples[idx]

def train_donut_model(input_json=None, output_dir=None, num_epochs=20, batch_size=1):
    """
    Train a Donut model on the provided dataset
    
    Args:
        input_json (str): Path to the training data JSON
        output_dir (str): Path to save the trained model
        num_epochs (int): Number of training epochs
        batch_size (int): Batch size for training
        
    Returns:
        str: Path to the saved model
    """
    if input_json is None:
        input_json = config.TRAIN_CLEANED_JSON
    
    if output_dir is None:
        output_dir = os.path.join(config.MODELS_DIR, f"donut_model_{int(time.time())}")
    
    # Create output directory
    config.ensure_dir(output_dir)
    
    # Load training data
    logger.info(f"Loading training data from {input_json}")
    with open(input_json, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    logger.info(f"Found {len(raw_data)} examples in the training data")
    
    # Initialize model and processor
    logger.info(f"Initializing model and processor from {config.DONUT_MODEL}")
    processor = DonutProcessor.from_pretrained(config.DONUT_MODEL)
    model = VisionEncoderDecoderModel.from_pretrained(config.DONUT_MODEL)
    
    # Set necessary configurations
    model.config.decoder_start_token_id = processor.tokenizer.pad_token_id
    model.config.pad_token_id = processor.tokenizer.pad_token_id
    
    # Prepare dataset
    logger.info("Preparing dataset")
    train_dataset = DonutDataset(raw_data, processor)
    logger.info(f"Dataset prepared with {len(train_dataset)} examples")
    
    # Set up training arguments
    training_args = Seq2SeqTrainingArguments(
        output_dir=output_dir,
        per_device_train_batch_size=batch_size,
        num_train_epochs=num_epochs,
        logging_steps=10,
        save_steps=50,
        save_total_limit=2,
        fp16=torch.cuda.is_available(),
        predict_with_generate=True,
        generation_max_length=128,
    )
    
    # Set up trainer
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        tokenizer=processor.tokenizer,
        data_collator=lambda data: {
            "pixel_values": torch.stack([f["pixel_values"] for f in data]),
            "labels": torch.nn.utils.rnn.pad_sequence(
                [f["labels"] for f in data],
                batch_first=True,
                padding_value=processor.tokenizer.pad_token_id
            )
        }
    )
    
    # Start training
    logger.info("Starting training")
    trainer.train()
    
    # Save final model
    logger.info(f"Training complete, saving model to {output_dir}")
    model.save_pretrained(output_dir)
    processor.save_pretrained(output_dir)
    
    return output_dir

def main():
    parser = argparse.ArgumentParser(description="Train Donut model for price comparison")
    parser.add_argument("--input_json", default=None, help="Path to the training data JSON")
    parser.add_argument("--output_dir", default=None, help="Path to save the trained model")
    parser.add_argument("--num_epochs", type=int, default=20, help="Number of training epochs")
    parser.add_argument("--batch_size", type=int, default=1, help="Batch size for training")
    args = parser.parse_args()
    
    model_path = train_donut_model(
        input_json=args.input_json,
        output_dir=args.output_dir,
        num_epochs=args.num_epochs,
        batch_size=args.batch_size
    )
    
    logger.info(f"Model training complete! Saved to: {model_path}")

if __name__ == "__main__":
    main()