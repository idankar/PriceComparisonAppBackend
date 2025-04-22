import os
import json
from PIL import Image
from datasets import Dataset
from transformers import DonutProcessor, VisionEncoderDecoderModel, Seq2SeqTrainer, Seq2SeqTrainingArguments
import torch

# Paths
DATA_DIR = "donut_data"
IMAGE_DIR = os.path.join(DATA_DIR, "images")
TRAIN_JSON = os.path.join(DATA_DIR, "train.json")
MODEL_NAME = "naver-clova-ix/donut-base"

# Load processor and model
processor = DonutProcessor.from_pretrained(MODEL_NAME)
model = VisionEncoderDecoderModel.from_pretrained(MODEL_NAME)
model.to("cuda" if torch.cuda.is_available() else "cpu")

# Load training data
with open(TRAIN_JSON, "r", encoding="utf-8") as f:
    raw_data = json.load(f)

# Format data for HuggingFace Dataset
def preprocess(example):
    image_path = os.path.join(IMAGE_DIR, example["image"])
    image = Image.open(image_path).convert("RGB")
    pixel_values = processor(image, return_tensors="pt").pixel_values.squeeze(0)

    target_str = json.dumps(example["label"], ensure_ascii=False)
    decoder_input_ids = processor.tokenizer(
        target_str,
        add_special_tokens=False,
        max_length=128,
        truncation=True,
        return_tensors="pt"
    ).input_ids.squeeze(0)

    return {
        "pixel_values": pixel_values,
        "labels": decoder_input_ids
    }

# Create dataset
hf_dataset = Dataset.from_list(raw_data)
processed_dataset = hf_dataset.map(preprocess)

# Training args
training_args = Seq2SeqTrainingArguments(
    output_dir="./donut_model",
    per_device_train_batch_size=1,
    num_train_epochs=20,
    logging_steps=10,
    save_steps=50,
    save_total_limit=2,
    fp16=torch.cuda.is_available(),
    predict_with_generate=True,
    generation_max_length=128,
)

# Trainer
trainer = Seq2SeqTrainer(
    model=model,
    args=training_args,
    train_dataset=processed_dataset,
    tokenizer=processor.tokenizer,
    data_collator=lambda data: {
        "pixel_values": torch.stack([f["pixel_values"] for f in data]),
        "labels": torch.nn.utils.rnn.pad_sequence([f["labels"] for f in data], batch_first=True, padding_value=processor.tokenizer.pad_token_id)
    }
)

trainer.train()
