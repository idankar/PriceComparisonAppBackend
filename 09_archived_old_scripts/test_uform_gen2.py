import torch
from transformers import AutoProcessor
from huggingface_hub import snapshot_download
from PIL import Image
import os
import importlib.util

# Step 1: Download the model
model_id = "unum-cloud/uform-gen2-qwen-500m"
local_dir = snapshot_download(repo_id=model_id)

# Step 2: Patch the import in modeling_uform_gen.py
model_file = os.path.join(local_dir, "modeling_uform_gen.py")
with open(model_file, "r") as f:
    lines = f.readlines()

patched_lines = [
    line.replace("from .configuration_uform_gen", "from configuration_uform_gen")
    for line in lines
]

with open(model_file, "w") as f:
    f.writelines(patched_lines)

# Step 3: Import configuration and model dynamically
config_file = os.path.join(local_dir, "configuration_uform_gen.py")
spec_cfg = importlib.util.spec_from_file_location("uform_cfg", config_file)
uform_cfg = importlib.util.module_from_spec(spec_cfg)
spec_cfg.loader.exec_module(uform_cfg)

spec = importlib.util.spec_from_file_location("uform_gen", model_file)
uform_gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(uform_gen)

# Step 4: Load model and processor
print("📦 Loading model and processor...")
model = uform_gen.UFormGenForConditionalGeneration.from_pretrained(local_dir, torch_dtype=torch.float32)
processor = AutoProcessor.from_pretrained(local_dir)

# Step 5: Load image
image_path = "קולה זירו.png"  # Replace if needed
image = Image.open(image_path).convert("RGB")

# Step 6: Prompt
prompt = "What is written on this product?"

# Step 7: Prepare input
inputs = processor(text=prompt, images=image, return_tensors="pt").to(model.device)

# Step 8: Generate output
print("🤖 Generating response...")
generate_ids = model.generate(**inputs, max_new_tokens=64)
output = processor.batch_decode(generate_ids, skip_special_tokens=True)[0]

print("\n🧠 Prediction:", output)
