# train_donut.py
import os
import argparse
import logging
from pathlib import Path
import json
import random
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    DonutProcessor, 
    VisionEncoderDecoderModel, 
    VisionEncoderDecoderConfig,
    Seq2SeqTrainer, 
    Seq2SeqTrainingArguments,
    get_scheduler
)
from datasets import load_metric
from PIL import Image
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("donut_training.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DonutDataset(Dataset):
    def __init__(self, metadata_file, processor, max_length=512):
        with open(metadata_file, 'r', encoding='utf-8') as f:
            self.metadata = [json.loads(line) for line in f]
        
        self.processor = processor
        self.max_length = max_length
        self.root_dir = os.path.dirname(metadata_file)
    
    def __len__(self):
        return len(self.metadata)
    
    def __getitem__(self, idx):
        item = self.metadata[idx]
        
        # Load image
        image_file = os.path.join(self.root_dir, item["image_path"])
        image = Image.open(image_file).convert("RGB")
        
        # Get label
        gt_parse = item["gt_parse"]
        
        # Preprocess
        encoding = self.processor(
            image, 
            gt_parse,
            padding="max_length",
            max_length=self.max_length, 
            truncation=True,
            return_tensors="pt"
        )
        
        # Remove batch dimension
        encoding = {k: v.squeeze() for k, v in encoding.items()}
        
        return encoding

def train_donut(args):
    """Train Donut model"""
    logger.info("Starting Donut training...")
    
    # Set up paths
    train_metadata = os.path.join(args.data_dir, "train", "metadata.jsonl")
    val_metadata = os.path.join(args.data_dir, "val", "metadata.jsonl")
    
    if not os.path.exists(train_metadata) or not os.path.exists(val_metadata):
        logger.error(f"Metadata files not found. Please run prepare_donut_dataset.py first.")
        return
    
    # Initialize processor and model
    if args.from_pretrained:
        logger.info(f"Loading pretrained model from {args.from_pretrained}")
        processor = DonutProcessor.from_pretrained(args.from_pretrained)
        model = VisionEncoderDecoderModel.from_pretrained(args.from_pretrained)
    else:
        logger.info("Initializing new model from Donut base model")
        processor = DonutProcessor.from_pretrained("naver-clova-ix/donut-base")
        model = VisionEncoderDecoderModel.from_pretrained("naver-clova-ix/donut-base")
        
        # Configure model
        # Set special tokens
        processor.tokenizer.add_special_tokens({"pad_token": "[PAD]"})
        model.config.decoder_start_token_id = processor.tokenizer.convert_tokens_to_ids(["[BOS]"])[0]
        model.config.pad_token_id = processor.tokenizer.pad_token_id
        # Set decoder config
        model.config.vocab_size = len(processor.tokenizer)
        
    # Create datasets
    train_dataset = DonutDataset(train_metadata, processor, max_length=args.max_length)
    val_dataset = DonutDataset(val_metadata, processor, max_length=args.max_length)
    
    logger.info(f"Loaded {len(train_dataset)} training examples, {len(val_dataset)} validation examples")
    
    # Set up training arguments
    training_args = Seq2SeqTrainingArguments(
        output_dir=os.path.join(args.output_dir, "checkpoints"),
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        num_train_epochs=args.num_epochs,
        logging_dir=os.path.join(args.output_dir, "logs"),
        logging_steps=100,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        save_total_limit=2,
        load_best_model_at_end=True,
        fp16=torch.cuda.is_available(),
        predict_with_generate=True
    )
    
    # Initialize Trainer
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
    )
    
    # Start training
    logger.info("Starting training...")
    trainer.train()
    
    # Save the final model
    model_dir = os.path.join(args.output_dir, "final_model")
    os.makedirs(model_dir, exist_ok=True)
    
    model.save_pretrained(model_dir)
    processor.save_pretrained(model_dir)
    
    logger.info(f"Training complete! Model saved to {model_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Donut model")
    parser.add_argument("--data-dir", default="data/donut", help="Directory containing Donut dataset")
    parser.add_argument("--output-dir", default="models/donut", help="Output directory for model")
    parser.add_argument("--batch-size", type=int, default=4, help="Batch size for training")
    parser.add_argument("--learning-rate", type=float, default=5e-5, help="Learning rate")
    parser.add_argument("--num-epochs", type=int, default=10, help="Number of training epochs")
    parser.add_argument("--max-length", type=int, default=512, help="Maximum sequence length")
    parser.add_argument("--from-pretrained", default=None, help="Path to pretrained model")
    
    args = parser.parse_args()
    
    train_donut(args)