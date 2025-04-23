# image_augmentation.py
import os
import cv2
import numpy as np
import random
from tqdm import tqdm

def augment_image(img):
    """Create variants of the image with different transformations to improve model robustness"""
    augmented = []
    
    # Original image
    augmented.append(img)
    
    # Rotate slightly
    for angle in [-3, 3]:
        h, w = img.shape[:2]
        M = cv2.getRotationMatrix2D((w/2, h/2), angle, 1)
        rotated = cv2.warpAffine(img, M, (w, h), borderMode=cv2.BORDER_REPLICATE)
        augmented.append(rotated)
    
    # Add slight perspective transformation
    h, w = img.shape[:2]
    pts1 = np.float32([[0,0], [w,0], [0,h], [w,h]])
    pts2 = np.float32([[0,0], [w,0], [10,h-10], [w-10,h-10]])
    M = cv2.getPerspectiveTransform(pts1, pts2)
    warped = cv2.warpPerspective(img, M, (w, h), borderMode=cv2.BORDER_REPLICATE)
    augmented.append(warped)
    
    # Change brightness
    bright = cv2.convertScaleAbs(img, alpha=1.2, beta=10)
    dark = cv2.convertScaleAbs(img, alpha=0.8, beta=-10)
    augmented.append(bright)
    augmented.append(dark)
    
    # Add slight blur
    blurred = cv2.GaussianBlur(img, (3, 3), 0)
    augmented.append(blurred)
    
    return augmented

def process_all_images(data_dir):
    """Augment all product images in the dataset"""
    print("Creating augmented images...")
    
    # Find all product image directories
    query_dirs = [os.path.join(data_dir, "cropped", "by_query", d) 
                 for d in os.listdir(os.path.join(data_dir, "cropped", "by_query"))
                 if os.path.isdir(os.path.join(data_dir, "cropped", "by_query", d))]
    
    total_augmented = 0
    
    for query_dir in query_dirs:
        print(f"Processing {os.path.basename(query_dir)}...")
        for filename in tqdm(os.listdir(query_dir)):
            if not filename.endswith(('.png', '.jpg', '.jpeg')):
                continue
                
            img_path = os.path.join(query_dir, filename)
            img = cv2.imread(img_path)
            
            if img is None:
                continue
                
            # Create augmented versions
            augmented_images = augment_image(img)
            
            # Save augmented images
            for i, aug_img in enumerate(augmented_images[1:]):  # Skip the first one (original)
                aug_filename = f"{os.path.splitext(filename)[0]}_aug{i}{os.path.splitext(filename)[1]}"
                aug_path = os.path.join(query_dir, aug_filename)
                cv2.imwrite(aug_path, aug_img)
                total_augmented += 1
    
    print(f"Created {total_augmented} augmented images")

if __name__ == "__main__":
    process_all_images("data")