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
from datasets import load_dataset, load_metric
from PIL import Image
import numpy as np

# Define paths relative to script or use absolute paths/config
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data/donut"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "models/donut_trained"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "donut_training.log"), # Log in project root
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DonutDataset(Dataset):
    """PyTorch Dataset for Donut."""

    def __init__(
        self,
        dataset_name_or_path: str,
        processor,
        max_length: int,
        split: str = "train",
        ignore_id: int = -100,
        task_start_token: str = "<s>",
        prompt_end_token: str = None,
    ):
        super().__init__()

        self.processor = processor
        self.max_length = max_length
        self.ignore_id = ignore_id
        self.task_start_token = task_start_token
        self.prompt_end_token = prompt_end_token if prompt_end_token else task_start_token

        # Load dataset from the specified path (expecting metadata.jsonl)
        self.dataset = load_dataset("json", data_files={split: os.path.join(dataset_name_or_path, split, "metadata.jsonl")})[split]
        self.dataset_length = len(self.dataset)
        # Absolute path to the image directory
        self.image_dir = Path(dataset_name_or_path) / split 
        # Correction: Images are expected in AUGMENTED_IMAGE_DIR based on prepare script
        self.image_dir = PROJECT_ROOT / "data/augmented" # Adjust if prepare script saves images differently

        self.added_tokens = []

    def __len__(self) -> int:
        return self.dataset_length

    def __getitem__(self, idx: int):
        sample = self.dataset[idx]

        # inputs
        # Ensure file_name exists and construct full path
        if "file_name" not in sample:
             raise ValueError(f"Missing 'file_name' key in dataset item at index {idx}: {sample}")
             
        image_path = self.image_dir / sample["file_name"]
        if not image_path.exists():
             raise FileNotFoundError(f"Image file not found for item {idx}: {image_path}")
             
        pixel_values = self.processor(Image.open(image_path).convert("RGB"), random_padding=True).pixel_values[0]

        # targets
        target_sequence = sample["ground_truth"]
        input_ids = self.processor.tokenizer(
            target_sequence,
            add_special_tokens=False,
            max_length=self.max_length,
            padding="max_length",
            truncation=True,
            return_tensors="pt",
        )["input_ids"].squeeze(0)

        labels = input_ids.clone()
        labels[labels == self.processor.tokenizer.pad_token_id] = self.ignore_id  # model doesn't need to predict pad token
        # labels[: T.shape[0]] = self.ignore_id # Not applicable with this tokenizer setup
        
        return {"pixel_values": pixel_values, "labels": labels}

def train_donut(args):
    """Train Donut model"""
    logger.info("Starting Donut training...")
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Output directory: {args.output_dir}")

    # Initialize processor and model
    if args.from_pretrained:
        logger.info(f"Loading pretrained model from {args.from_pretrained}")
        try:
             processor = DonutProcessor.from_pretrained(args.from_pretrained)
             model = VisionEncoderDecoderModel.from_pretrained(args.from_pretrained)
        except Exception as e:
             logger.error(f"Failed to load pretrained model: {e}")
             return
    else:
        logger.info("Initializing new model from naver-clova-ix/donut-base-finetuned-cord-v2")
        try:
             # Using a finetuned base model is often better than the raw base
             model_name = "naver-clova-ix/donut-base-finetuned-cord-v2"
             processor = DonutProcessor.from_pretrained(model_name)
             model = VisionEncoderDecoderModel.from_pretrained(model_name)
        except Exception as e:
             logger.error(f"Failed to load base model: {e}")
             return

    # Resize token embeddings if necessary (if new tokens were added)
    # model.resize_token_embeddings(len(processor.tokenizer))

    # Update model config
    model.config.decoder_start_token_id = processor.tokenizer.bos_token_id
    model.config.pad_token_id = processor.tokenizer.pad_token_id
    # model.config.vocab_size = model.config.decoder.vocab_size

    # Create datasets
    try:
         train_dataset = DonutDataset(
             args.data_dir,
             processor=processor,
             max_length=args.max_length,
             split="train",
             task_start_token=processor.tokenizer.bos_token # Use processor's defined start token
         )
         val_dataset = DonutDataset(
             args.data_dir,
             processor=processor,
             max_length=args.max_length,
             split="val",
             task_start_token=processor.tokenizer.bos_token
         )
         logger.info(f"Loaded {len(train_dataset)} training examples, {len(val_dataset)} validation examples")
    except Exception as e:
         logger.error(f"Failed to load datasets: {e}. Ensure metadata.jsonl exists in train/ and val/ subdirs of {args.data_dir}")
         return

    # Set up training arguments
    # Ensure output_dir is a string
    output_dir_str = str(args.output_dir)
    training_args = Seq2SeqTrainingArguments(
        output_dir=os.path.join(output_dir_str, "checkpoints"),
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        num_train_epochs=args.num_epochs,
        logging_dir=os.path.join(output_dir_str, "logs"),
        logging_steps=args.log_steps,
        evaluation_strategy="steps",
        eval_steps=args.eval_steps, # Evaluate periodically
        save_strategy="steps",
        save_steps=args.save_steps, # Save checkpoints periodically
        save_total_limit=args.save_limit,
        load_best_model_at_end=True,
        metric_for_best_model="eval_loss", # Using loss for simplicity
        greater_is_better=False,
        fp16=torch.cuda.is_available(),
        report_to="tensorboard", # Log to tensorboard
        predict_with_generate=True # Needed for evaluation
    )

    # Initialize Trainer
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        processor=processor # Pass processor for generation during eval
    )

    # Start training
    logger.info("Starting training...")
    try:
         trainer.train()
    except Exception as e:
         logger.error(f"Training failed: {e}")
         return

    # Save the final best model
    final_model_dir = Path(output_dir_str) / "final_model"
    final_model_dir.mkdir(parents=True, exist_ok=True)

    try:
         trainer.save_model(str(final_model_dir)) # Use trainer's save method
         processor.save_pretrained(str(final_model_dir))
         logger.info(f"Training complete! Final model saved to {final_model_dir}")
    except Exception as e:
         logger.error(f"Failed to save final model: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Donut model")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Directory containing Donut dataset (train/val subdirs with metadata.jsonl)")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for model checkpoints and logs")
    parser.add_argument("--from-pretrained", default=None, help="Path to a local pretrained model/checkpoint to continue training")
    parser.add_argument("--batch-size", type=int, default=2, help="Batch size per device for training/evaluation")
    parser.add_argument("--learning-rate", type=float, default=5e-5, help="Initial learning rate")
    parser.add_argument("--num-epochs", type=int, default=5, help="Number of training epochs")
    parser.add_argument("--max-length", type=int, default=768, help="Maximum sequence length for processor") # Increased max length
    parser.add_argument("--log-steps", type=int, default=100, help="Log training info every N steps")
    parser.add_argument("--eval-steps", type=int, default=500, help="Evaluate model every N steps")
    parser.add_argument("--save-steps", type=int, default=500, help="Save checkpoint every N steps")
    parser.add_argument("--save-limit", type=int, default=2, help="Maximum number of checkpoints to keep")

    args = parser.parse_args()

    # Ensure output directory exists
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    train_donut(args)
