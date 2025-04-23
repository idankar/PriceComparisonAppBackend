# Create the detector.py file in src directory
cat > src/detector.py << 'EOL'
#!/usr/bin/env python3
# src/detector.py - YOLO-based product detection and cropping

import os
import sys
import glob
import logging
import cv2
from ultralytics import YOLO

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
    
    cropped_images = []
    
    # Process each screenshot
    for i, image_path in enumerate(screenshot_paths):
        logger.info(f"Processing screenshot {i+1}/{len(screenshot_paths)}: {os.path.basename(image_path)}")
        
        # Read image
        image = cv2.imread(image_path)
        if image is None:
            logger.error(f"Failed to read image: {image_path}")
            continue
        
        # Run YOLO detection
        results = model(image)
        
        # Extract filename parts for unique cropped image names
        screenshot_basename = os.path.basename(image_path)
        screenshot_name = os.path.splitext(screenshot_basename)[0]
        
        # Process each detected object
        for j, result in enumerate(results):
            boxes = result.boxes.xyxy.cpu().numpy()
            
            for k, box in enumerate(boxes):
                # Create unique name for the cropped product
                if run_id:
                    product_name = f"product_{run_id}_{screenshot_name}_{k}.png"
                else:
                    product_name = f"product_{screenshot_name}_{k}.png"
                
                out_path = os.path.join(output_dir, product_name)
                
                # Apply padding to the bounding box
                x1 = max(int(box[0]) - 40, 0)            # left pad
                y1 = max(int(box[1]) - 20, 0)            # top pad
                x2 = min(int(box[2]) + 100, image.shape[1])  # right pad
                y2 = min(int(box[3]) + 180, image.shape[0])  # bottom pad (boosted)
                
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
    
    # Run product detection and cropping
    return crop_products_from_screenshots(
        screenshots_dir=paths["screenshots_dir"],
        output_dir=paths["cropped_dir"],
        run_id=paths["run_id"]
    )

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("⚠️ Missing query. Usage: python detector.py 'מעדן'")
        sys.exit(1)
    
    query = sys.argv[1]
    cropped_images = process_query_screenshots(query)
    logger.info(f"✅ Detected and cropped {len(cropped_images)} product images for query: {query}")
EOL