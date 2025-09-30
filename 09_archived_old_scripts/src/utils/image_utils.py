#!/usr/bin/env python3
# image_utils.py - Utility functions for image processing

import os
import requests
import logging
import numpy as np
from PIL import Image, ImageEnhance, ImageOps, ImageFilter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_image(url, save_path):
    """Download an image from a URL to a local path
    
    Args:
        url (str): URL of the image
        save_path (str): Local path to save the image
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Download the image
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        # Save the image
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Downloaded image: {save_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading image: {e}")
        return False

def resize_image(image_path, target_size=(224, 224), save_path=None):
    """Resize an image to a target size
    
    Args:
        image_path (str): Path to the image
        target_size (tuple): Target size as (width, height)
        save_path (str, optional): Path to save the resized image
        
    Returns:
        PIL.Image: Resized image
    """
    try:
        # Open the image
        image = Image.open(image_path).convert('RGB')
        
        # Resize the image
        resized_image = image.resize(target_size, Image.Resampling.LANCZOS)
        
        # Save the image if requested
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            resized_image.save(save_path)
            logger.info(f"Saved resized image: {save_path}")
        
        return resized_image
        
    except Exception as e:
        logger.error(f"Error resizing image: {e}")
        return None

def augment_image(image_path, save_dir=None, num_augmentations=5):
    """Generate augmented versions of an image
    
    Args:
        image_path (str): Path to the image
        save_dir (str, optional): Directory to save augmented images
        num_augmentations (int): Number of augmentations to generate
        
    Returns:
        list: List of augmented images or paths to saved images
    """
    try:
        # Open the image
        image = Image.open(image_path).convert('RGB')
        
        # Get the base filename
        base_name = os.path.splitext(os.path.basename(image_path))[0]
        
        # Prepare augmentation functions
        augmentations = [
            lambda img: ImageEnhance.Brightness(img).enhance(np.random.uniform(0.8, 1.2)),
            lambda img: ImageEnhance.Contrast(img).enhance(np.random.uniform(0.8, 1.2)),
            lambda img: ImageEnhance.Color(img).enhance(np.random.uniform(0.8, 1.2)),
            lambda img: img.rotate(np.random.uniform(-15, 15)),
            lambda img: img.filter(ImageFilter.GaussianBlur(radius=np.random.uniform(0, 1))),
            lambda img: ImageOps.mirror(img)
        ]
        
        augmented_images = []
        
        for i in range(num_augmentations):
            # Apply random augmentations
            aug_image = image.copy()
            for aug_func in augmentations:
                if np.random.random() > 0.5:
                    aug_image = aug_func(aug_image)
            
            # Save the image if requested
            if save_dir:
                os.makedirs(save_dir, exist_ok=True)
                save_path = os.path.join(save_dir, f"{base_name}_aug_{i+1}.jpg")
                aug_image.save(save_path)
                augmented_images.append(save_path)
                logger.info(f"Saved augmented image: {save_path}")
            else:
                augmented_images.append(aug_image)
        
        return augmented_images
        
    except Exception as e:
        logger.error(f"Error augmenting image: {e}")
        return []

def crop_product_from_image(image_path, save_path=None):
    """Attempt to crop the product from an image using simple heuristics
    
    Args:
        image_path (str): Path to the image
        save_path (str, optional): Path to save the cropped image
        
    Returns:
        PIL.Image: Cropped image or None if unsuccessful
    """
    try:
        # Open the image
        image = Image.open(image_path).convert('RGB')
        
        # Convert to numpy array for processing
        image_array = np.array(image)
        
        # Simple background detection - assumes product is in center
        # and background is relatively uniform
        
        # Get the image dimensions
        height, width, _ = image_array.shape
        
        # Sample the corners to estimate background color
        corner_pixels = [
            image_array[0, 0],          # Top-left
            image_array[0, width-1],    # Top-right
            image_array[height-1, 0],   # Bottom-left
            image_array[height-1, width-1]  # Bottom-right
        ]
        
        # Convert to grayscale for simpler comparison
        corner_grays = [int(np.mean(pixel)) for pixel in corner_pixels]
        avg_bg = np.mean(corner_grays)
        
        # Create a mask where pixels different from background are marked
        mask = np.zeros((height, width), dtype=np.uint8)
        gray_image = np.mean(image_array, axis=2)
        
        # Mark pixels that differ from background by a threshold
        threshold = 30  # Adjust this value as needed
        mask = np.abs(gray_image - avg_bg) > threshold
        
        # Find the bounding box of the mask
        rows = np.any(mask, axis=1)
        cols = np.any(mask, axis=0)
        
        # If no product is detected, return the original image
        if not np.any(rows) or not np.any(cols):
            logger.warning("Could not detect product in image")
            return image
        
        # Get the bounds
        y_min, y_max = np.where(rows)[0][[0, -1]]
        x_min, x_max = np.where(cols)[0][[0, -1]]
        
        # Add some padding
        padding = 10
        y_min = max(0, y_min - padding)
        y_max = min(height - 1, y_max + padding)
        x_min = max(0, x_min - padding)
        x_max = min(width - 1, x_max + padding)
        
        # Crop the image
        cropped_image = image.crop((x_min, y_min, x_max, y_max))
        
        # Save the image if requested
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            cropped_image.save(save_path)
            logger.info(f"Saved cropped image: {save_path}")
        
        return cropped_image
        
    except Exception as e:
        logger.error(f"Error cropping image: {e}")
        return None 