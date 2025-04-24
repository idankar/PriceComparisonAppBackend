# train.py
import os
import argparse
import logging
import torch
import json
from transformers import DonutProcessor, VisionEncoderDecoderModel, Seq2SeqTrainer, Seq2SeqTrainingArguments
from datasets import Dataset
import config

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

def load_dataset(json_path):
    """
    Load the dataset from the JSON file
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Convert to HuggingFace dataset format
    examples = []
    for item in data:
        image_path = item["image_path"]
        # Convert ground truth to string
        gt_str = json.dumps(item["ground_truth"], ensure_ascii=False)
        
        examples.append({
            "image_path": image_path,
            "ground_truth": gt_str
        })
    
    return Dataset.from_list(examples)

def train_donut(args):
    """
    Train the Donut model
    """
    logger.info("Starting Donut training")
    
    # Load datasets
    train_json = os.path.join(config.DONUT_TRAIN_DIR, "train.json")
    val_json = os.path.join(config.DONUT_VAL_DIR, "val.json")
    
    train_dataset = load_dataset(train_json)
    val_dataset = load_dataset(val_json)
    
    logger.info(f"Loaded {len(train_dataset)} training examples and {len(val_dataset)} validation examples")
    
    # Initialize processor and model
    processor = DonutProcessor.from_pretrained("naver-clova-ix/donut-base")
    model = VisionEncoderDecoderModel.from_pretrained("naver-clova-ix/donut-base")
    
    # Set special tokens
    processor.tokenizer.add_special_tokens({"pad_token": "[PAD]"})
    model.config.decoder_start_token_id = processor.tokenizer.convert_tokens_to_ids(["[BOS]"])[0]
    model.config.pad_token_id = processor.tokenizer.pad_token_id
    
    # Prepare dataset for training
    def preprocess_data(examples):
        images = [os.path.join(os.getcwd(), image_path) for image_path in examples["image_path"]]
        texts = examples["ground_truth"]
        
        # Encode the images and texts
        encoding = processor(
            images=images, 
            text=texts,
            padding="max_length",
            max_length=512,
            truncation=True,
            return_tensors="pt"
        )
        
        return {
            "pixel_values": encoding.pixel_values,
            "labels": encoding.labels
        }
    
    # Apply preprocessing
    train_dataset = train_dataset.map(preprocess_data, batched=True, batch_size=args.batch_size)
    val_dataset = val_dataset.map(preprocess_data, batched=True, batch_size=args.batch_size)
    
    # Set training arguments
    training_args = Seq2SeqTrainingArguments(
        output_dir=os.path.join(config.DONUT_MODEL_DIR, "checkpoints"),
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        predict_with_generate=True,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        num_train_epochs=args.num_epochs,
        fp16=torch.cuda.is_available(),
        logging_dir=os.path.join(config.DONUT_MODEL_DIR, "logs"),
        logging_steps=100,
        save_total_limit=2,
    )
    
    # Initialize trainer
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
    )
    
    # Train the model
    logger.info("Starting training...")
    trainer.train()
    
    # Save the model
    output_dir = os.path.join(config.DONUT_MODEL_DIR, "final")
    os.makedirs(output_dir, exist_ok=True)
    
    model.save_pretrained(output_dir)
    processor.save_pretrained(output_dir)
    
    logger.info(f"Model saved to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Donut model")
    parser.add_argument("--num_epochs", type=int, default=30, help="Number of training epochs")
    parser.add_argument("--batch_size", type=int, default=4, help="Batch size for training")
    
    args = parser.parse_args()
    
    train_donut(args)