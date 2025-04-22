from ultralytics import YOLO
import cv2
import os

image_path = "/Users/noa/Desktop/PriceComparisonApp/screenshots/screenshot_0_0.png"
output_dir = "cropped_products"
os.makedirs(output_dir, exist_ok=True)

image = cv2.imread(image_path)
if image is None:
    raise FileNotFoundError(f"Image not found: {image_path}")

model = YOLO("yolov8n.pt")
results = model(image)

for i, result in enumerate(results):
    boxes = result.boxes.xyxy.cpu().numpy()
    for j, box in enumerate(boxes):
        x1 = max(int(box[0]) - 40, 0)                        # left pad
        y1 = max(int(box[1]) - 20, 0)                        # top pad
        x2 = min(int(box[2]) + 100, image.shape[1])         # right pad
        y2 = min(int(box[3]) + 180, image.shape[0])         # bottom pad 🔼 boosted

        cropped = image[y1:y2, x1:x2]
        out_path = os.path.join(output_dir, f"product_{j}.png")
        cv2.imwrite(out_path, cropped)
        print(f"✅ Saved: {out_path}")
