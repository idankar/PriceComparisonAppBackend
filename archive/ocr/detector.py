#!/usr/bin/env python3
# src/detector.py - YOLO-based product detection and cropping

import os
import sys
import glob
import logging
import cv2
from ultralytics import YOLO
import numpy as np
import time

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(config.LOGS_DIR, "detector.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_nutella_search_page(image_path, output_dir, run_id=None):
    """
    Process a Nutella search results page using a targeted grid approach
    focused on the product area
    """
    image = cv2.imread(image_path)
    if image is None:
        logger.error(f"Failed to read image: {image_path}")
        return []
    
    height, width = image.shape[:2]
    
    # For Nutella search results, products typically start after the header
    # and are arranged in a grid of 3-4 columns
    
    # Skip the top navigation (~15% of the page)
    start_y = int(height * 0.15)
    
    # Skip headers and search bar (~10% more)
    start_y += int(height * 0.10)
    
    # Use up to 60% of the remaining height (covers product area)
    usable_height = int((height - start_y) * 0.6)
    
    # Define a 4-column, 2-row grid for products
    cols = 4
    rows = 2
    
    cell_width = width // cols
    cell_height = usable_height // rows
    
    cropped_images = []
    
    for row in range(rows):
        for col in range(cols):
            # Calculate cell coordinates
            x1 = col * cell_width
            y1 = start_y + (row * cell_height)
            x2 = x1 + cell_width
            y2 = y1 + cell_height
            
            # Skip if the cell is too small
            if (x2 - x1) < 100 or (y2 - y1) < 100:
                continue
            
            # Create a unique name
            screenshot_basename = os.path.basename(image_path)
            product_name = f"nutella_grid_{run_id}_{screenshot_basename}_{row}_{col}.png"
            out_path = os.path.join(output_dir, product_name)
            
            # Crop and save
            cropped = image[y1:y2, x1:x2]
            cv2.imwrite(out_path, cropped)
            cropped_images.append(out_path)
            logger.info(f"✅ Nutella Grid Saved: {out_path}")
    
    return cropped_images

def detect_product_grid(image):
    """
    Use edge detection and contour finding to identify product tile grid
    specifically optimized for Shufersal layout
    """
    # Convert to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    # Apply bilateral filter to reduce noise while preserving edges
    blurred = cv2.bilateralFilter(gray, 11, 17, 17)
    
    # Apply Canny edge detection
    edges = cv2.Canny(blurred, 30, 200)
    
    # Dilate edges to connect nearby edges
    kernel = np.ones((3,3), np.uint8)
    dilated = cv2.dilate(edges, kernel, iterations=1)
    
    # Find contours
    contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    # Sort contours by area (largest first)
    contours = sorted(contours, key=cv2.contourArea, reverse=True)
    
    # Filter for product-sized rectangular contours
    img_height, img_width = image.shape[:2]
    product_boxes = []
    
    # Shufersal product tiles are usually approximately 250x400 pixels
    # but screenshots may vary in size, so use relative sizes
    min_width = img_width * 0.15  # Min 15% of image width
    max_width = img_width * 0.33  # Max 33% of image width
    min_height = img_height * 0.25  # Min 25% of image height
    max_height = img_height * 0.6  # Max 60% of image height
    
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        
        # Check if dimensions are in the expected range for a product tile
        if (min_width < w < max_width and min_height < h < max_height):
            # For debugging
            # logger.info(f"Found product box via contour: x={x}, y={y}, w={w}, h={h}") # Removed debug log
            # Return tuple for consistency with YOLO boxes
            product_boxes.append(tuple(map(int, (x, y, x + w, y + h)))) 
    
    # If no products found with contour detection, try a fixed grid approach
    if not product_boxes:
        logger.info("No products found via contour detection, trying fixed grid")
        # Shufersal typically displays products in a grid of 3-4 columns
        cols = 4
        rows = 2
        
        # Calculate the dimensions of each grid cell
        cell_width = img_width // cols
        cell_height = img_height // rows
        
        # Create grid cells
        for row in range(rows):
            for col in range(cols):
                x = col * cell_width
                y = row * cell_height
                
                # Add some padding inside the cell
                padding_x = cell_width * 0.1
                padding_y = cell_height * 0.1
                
                # Create box with padding, ensure coordinates are integers
                box = tuple(map(int, (
                    x + padding_x,
                    y + padding_y,
                    x + cell_width - padding_x,
                    y + cell_height - padding_y
                )))
                product_boxes.append(box)
        
        logger.info(f"Added {len(product_boxes)} boxes with fixed grid")
    else:
        logger.info(f"Detected {len(product_boxes)} potential grid boxes via contours")

    return product_boxes

def crop_products_from_screenshots(screenshots_dir, output_dir, run_id=None):
    """
    Use YOLO to detect and crop product images from screenshots
    
    Args:
        screenshots_dir (str): Directory containing screenshots
        output_dir (str): Directory to save cropped products
        run_id (str, optional): Unique identifier for this run
        
    Returns:
        list: Paths to cropped product images
    """
    # Load YOLO model
    logger.info("🔍 Loading YOLO model...")
    model = YOLO(config.YOLO_MODEL)
    
    # Find all PNG files in screenshots_dir
    screenshot_paths = glob.glob(os.path.join(screenshots_dir, "*.png"))
    
    if not screenshot_paths:
        logger.warning(f"No screenshot files found in {screenshots_dir}")
        return []
    
    logger.info(f"Found {len(screenshot_paths)} screenshots to process")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Ensure debug directory exists for saving annotated images
    debug_dir = os.path.join("data", "debug_detections")
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    cropped_images = []
    
    # Process each screenshot
    for i, image_path in enumerate(screenshot_paths):
        logger.info(f"Processing screenshot {i+1}/{len(screenshot_paths)}: {os.path.basename(image_path)}")
        
        # Read image
        image = cv2.imread(image_path)
        if image is None:
            logger.error(f"Failed to read image: {image_path}")
            continue

        # Resize image to ensure YOLO works optimally
        # Shufersal pages are often quite large, so scale down if needed
        max_dim = 1280
        height, width = image.shape[:2]
        if max(height, width) > max_dim:
            scale = max_dim / max(height, width)
            new_width = int(width * scale)
            new_height = int(height * scale)
            image = cv2.resize(image, (new_width, new_height))
            logger.info(f"Resized image from {width}x{height} to {new_width}x{new_height}")

        # Run YOLO detection with optimized parameters for retail products
        results = model(
            image,
            conf=0.25,          # Lower confidence threshold 
            iou=0.45,           # Standard IOU threshold
            agnostic_nms=True   # Corrected parameter name (was agnostic)
        )
        
        # ---- START: Visualize YOLO Detections ----
        screenshot_basename = os.path.basename(image_path)
        screenshot_name = os.path.splitext(screenshot_basename)[0]
        debug_save_path = os.path.join(debug_dir, f"{screenshot_name}_yolo_debug.png")
        try:
            if results and results[0].boxes:
                 annotated_image = results[0].plot() # Returns numpy array with boxes drawn
                 cv2.imwrite(debug_save_path, annotated_image)
                 logger.info(f"Saved YOLO detection visualization to: {debug_save_path}")
            else:
                 # Save original (resized) image if no detections
                 # cv2.imwrite(debug_save_path.replace("_debug", "_no_detect"), image)
                 logger.info("No YOLO detections to visualize.")
        except Exception as e:
            logger.error(f"Error saving debug image {debug_save_path}: {e}")
        # ---- END: Visualize YOLO Detections ----

        # Extract YOLO boxes
        yolo_boxes = []
        for result in results: # Iterate through potential multiple results (though likely 1)
            if result.boxes:
                for box in result.boxes:
                    x1, y1, x2, y2 = box.xyxy.cpu().numpy()[0]
                    # Ensure box coordinates are within image bounds after potential resize
                    h_img, w_img = image.shape[:2]
                    x1 = max(0, int(x1))
                    y1 = max(0, int(y1))
                    x2 = min(w_img, int(x2))
                    y2 = min(h_img, int(y2))
                    # Append as list [x1, y1, x2, y2]
                    yolo_boxes.append([x1, y1, x2, y2])

        logger.info(f"YOLO detected {len(yolo_boxes)} boxes")

        # Get grid boxes
        # Ensure grid boxes are also lists [x1, y1, x2, y2]
        grid_boxes_tuples = detect_product_grid(image) 
        grid_boxes = [list(b) for b in grid_boxes_tuples]
        logger.info(f"Detected {len(grid_boxes)} potential grid boxes")

        # Combine boxes
        all_boxes = yolo_boxes + grid_boxes
        logger.info(f"Total boxes after combining YOLO and grid detection: {len(all_boxes)}")

        # Remove overlapping boxes using cv2.dnn.NMSBoxes
        final_boxes = []
        if all_boxes:
            # NMSBoxes expects boxes in [x, y, width, height] format
            # And confidences
            boxes_xywh = []
            confidences = []
            for box in all_boxes:
                x1, y1, x2, y2 = box
                boxes_xywh.append([x1, y1, x2 - x1, y2 - y1])
                # Assign confidence 1.0 to all boxes for NMS
                # Could potentially use YOLO confidences if available
                confidences.append(1.0)
            
            # Use NMS to remove overlapping boxes
            # score_threshold can be low as we are using dummy confidences
            # nms_threshold controls overlap removal
            indices = cv2.dnn.NMSBoxes(
                boxes_xywh, 
                confidences,
                score_threshold=0.1,  
                nms_threshold=0.4   
            )
            
            # Extract final boxes using the indices
            if indices is not None: 
                 # Handle potential different return types of indices
                 indices_flat = indices.flatten() if hasattr(indices, 'flatten') else indices
                 final_boxes = [all_boxes[i] for i in indices_flat]

        logger.info(f"Kept {len(final_boxes)} boxes after removing overlaps")
        # all_boxes = final_boxes # Replace previous NMS filtering line

        # Extract filename parts for unique cropped image names
        screenshot_basename = os.path.basename(image_path)
        screenshot_name = os.path.splitext(screenshot_basename)[0]
        
        # Process each unique box (use final_boxes now)
        for k, box in enumerate(final_boxes):
            # Create unique name for the cropped product
            if run_id:
                product_name = f"product_{run_id}_{screenshot_name}_{k}.png"
            else:
                product_name = f"product_{screenshot_name}_{k}.png"
            
            out_path = os.path.join(output_dir, product_name)
            
            # Apply padding to the bounding box - reduce padding to get tighter crops
            x1 = max(int(box[0]) - 10, 0)            # left pad
            y1 = max(int(box[1]) - 10, 0)            # top pad
            x2 = min(int(box[2]) + 10, image.shape[1])  # right pad
            y2 = min(int(box[3]) + 40, image.shape[0])  # bottom pad

            # Optional: Add size filtering to avoid tiny crops
            if (x2 - x1) < 50 or (y2 - y1) < 50:  # Skip if the crop is too small
                continue
            
            # Crop the image
            cropped = image[y1:y2, x1:x2]
            
            # Save the cropped image
            cv2.imwrite(out_path, cropped)
            cropped_images.append(out_path)
            logger.info(f"✅ Saved: {out_path}")
    
    logger.info(f"Completed processing {len(screenshot_paths)} screenshots, extracted {len(cropped_images)} product images")
    return cropped_images

def process_query_screenshots(query):
    """
    Process all screenshots for a specific query
    
    Args:
        query (str): Search query that was used
        
    Returns:
        list: Paths to cropped product images
    """
    # Get query-specific paths
    paths = config.get_query_paths(query)
    
    # Find all PNG files in screenshots_dir
    screenshot_paths = glob.glob(os.path.join(paths["screenshots_dir"], "*.png"))
    
    if not screenshot_paths:
        logger.warning(f"No screenshot files found in {paths['screenshots_dir']}")
        return []
    
    logger.info(f"Found {len(screenshot_paths)} screenshots to process")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(paths["cropped_dir"]):
        os.makedirs(paths["cropped_dir"])
    
    cropped_images = []
    
    # Use specialized processing for Nutella queries
    if query.lower() in ["נוטלה", "nutella"]:
        for screenshot_path in screenshot_paths:
            nutella_crops = process_nutella_search_page(
                screenshot_path, 
                paths["cropped_dir"], 
                paths["run_id"]
            )
            cropped_images.extend(nutella_crops)
    else:
        # Use standard processing for other queries
        # Note: crop_products_from_screenshots expects a directory, not a list of paths
        # We need to adjust the call or the function signature.
        # For now, let's assume crop_products_from_screenshots is called as before
        # This requires modification in crop_products_from_screenshots 
        # OR we need to modify this function to loop like the Nutella part.
        # Reverting to original call structure for now, but this needs attention.
        cropped_images = crop_products_from_screenshots(
            paths["screenshots_dir"], # Pass the directory again
            paths["cropped_dir"],
            paths["run_id"]
        )
    
    return cropped_images

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️ Missing query. Usage: python detector.py 'מעדן'")
        sys.exit(1)
    
    query = sys.argv[1]
    cropped_images = process_query_screenshots(query)
    logger.info(f"✅ Detected and cropped {len(cropped_images)} product images for query: {query}")