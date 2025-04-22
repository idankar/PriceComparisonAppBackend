from transformers import DonutProcessor, VisionEncoderDecoderModel
from PIL import Image
import torch

# Load processor + model
processor = DonutProcessor.from_pretrained("naver-clova-ix/donut-base-finetuned-docvqa")
model = VisionEncoderDecoderModel.from_pretrained("naver-clova-ix/donut-base-finetuned-docvqa")

device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

# Load image
image_path = "screenshots/screenshot_0_0.png"
image = Image.open(image_path).convert("RGB")

# Structured prompt
prompt = "<s_docvqa><question>What is the name of the food product in Hebrew, and what is its price in Shekels?</question><answer>"

# Tokenize and forward
pixel_values = processor(image, prompt, return_tensors="pt").pixel_values.to(device)
outputs = model.generate(
    pixel_values,
    max_length=512,
    num_beams=3,
    early_stopping=True,
    pad_token_id=processor.tokenizer.pad_token_id
)

# Decode result
result = processor.batch_decode(outputs, skip_special_tokens=True)[0]

# Display
print("\n🧠 Donut OCR Output:")
print("----------------------------------------")
print(result.strip())
print("----------------------------------------")
