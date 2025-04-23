from transformers import DonutProcessor, VisionEncoderDecoderModel
from PIL import Image
import torch
import sys


class DonutOCR:
    def __init__(self, model_name: str = "naver-clova-ix/donut-base"):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = VisionEncoderDecoderModel.from_pretrained(model_name).to(self.device)
        self.processor = DonutProcessor.from_pretrained(model_name)
        self.model.eval()

    def run(self, image_path: str, prompt: str = "Extract product name and price in Hebrew.\nOutput format: {'name': ..., 'price': ...}") -> str:
        image = Image.open(image_path).convert("RGB")
        pixel_values = self.processor(image, return_tensors="pt").pixel_values.to(self.device)

        decoder_input_ids = self.processor.tokenizer(
            prompt,
            add_special_tokens=False,
            return_tensors="pt"
        ).input_ids.to(self.device)

        outputs = self.model.generate(
            pixel_values,
            decoder_input_ids=decoder_input_ids,
            max_length=128,
            early_stopping=True,
            pad_token_id=self.processor.tokenizer.pad_token_id
        )

        decoded = self.processor.batch_decode(outputs, skip_special_tokens=True)[0]
        return decoded


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python donut_wrapper.py path/to/image.png")
        sys.exit(1)

    image_path = sys.argv[1]
    donut = DonutOCR()
    result = donut.run(image_path)
    print("Donut result:", result)
