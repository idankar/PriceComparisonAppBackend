import cv2
import matplotlib.pyplot as plt
import os

# Path to cropped image
image_path = "cropped_products/product_0.png"

# Check if file exists
if not os.path.exists(image_path):
    raise FileNotFoundError(f"Image not found: {image_path}")

# Read image with OpenCV
image = cv2.imread(image_path)

# Convert BGR (OpenCV format) to RGB (matplotlib format)
image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

# Display using matplotlib
plt.imshow(image_rgb)
plt.title("Cropped Product")
plt.axis("off")
plt.tight_layout()
plt.show()
