# scripts/run_pipeline.py
import os
import sys
import subprocess
import logging
import argparse
from pathlib import Path

# Define paths relative to script or use absolute paths/config
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "pipeline.log"), # Log in project root
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration --- 
# (Can be moved to a config file or made into more CLI args)
RAW_IMAGE_SOURCE_DIR = PROJECT_ROOT / "data/raw_images"
LABELED_DATA_OUTPUT_FILE = PROJECT_ROOT / "data/labeled/labeled_data.json"
AUGMENTED_IMAGE_OUTPUT_DIR = PROJECT_ROOT / "data/augmented"
AUGMENTED_METADATA_OUTPUT_FILE = AUGMENTED_IMAGE_OUTPUT_DIR / "augmented_metadata.json"
DONUT_DATA_OUTPUT_DIR = PROJECT_ROOT / "data/donut"
TRAINED_MODEL_OUTPUT_DIR = PROJECT_ROOT / "models/donut_trained"

# --- Helper Function --- 
def run_script(script_name, args_list):
    """Runs a python script as a subprocess and checks for errors."""
    script_path = SCRIPT_DIR / script_name
    command = [sys.executable, str(script_path)] + args_list
    logger.info(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        logger.info(f"Script {script_name} completed successfully.")
        logger.debug(f"Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Script {script_name} failed with exit code {e.returncode}.")
        logger.error(f"Stderr:\n{e.stderr}")
        logger.error(f"Stdout:\n{e.stdout}")
        return False
    except FileNotFoundError:
        logger.error(f"Script not found: {script_path}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while running {script_name}: {e}")
        return False

# --- Main Pipeline --- 
def main(args):
    logger.info("Starting the full data processing and training pipeline...")

    # == Step 1: Labeling with GPT-4o ==
    if not args.skip_labeling:
        logger.info("\n--- Running GPT-4o Vision Labeling ---")
        labeler_args = [
            "--image-dir", str(RAW_IMAGE_SOURCE_DIR),
            "--output-file", str(LABELED_DATA_OUTPUT_FILE),
            "--max-images", str(args.max_label_images) # Pass max images arg
            # Add other necessary args for the labeler (e.g., API key, prompt)
        ]
        if not run_script("gpt4_vision_labeler.py", labeler_args):
            logger.error("GPT-4o labeling failed. Exiting pipeline.")
            sys.exit(1)
        logger.info("--- GPT-4o Labeling Complete ---")
    else:
        logger.info("Skipping GPT-4o labeling step.")
        if not LABELED_DATA_OUTPUT_FILE.exists():
             logger.error(f"Skipped labeling, but required input file {LABELED_DATA_OUTPUT_FILE} not found!")
             sys.exit(1)

    # == Step 2: Augmentation ==
    if not args.skip_augmentation:
        logger.info("\n--- Running Image Augmentation ---")
        augment_args = [
            "--labeled-file", str(LABELED_DATA_OUTPUT_FILE),
            "--raw-dir", str(RAW_IMAGE_SOURCE_DIR), # Source for original images
            "--aug-dir", str(AUGMENTED_IMAGE_OUTPUT_DIR),
            "--meta-out", str(AUGMENTED_METADATA_OUTPUT_FILE),
            "--workers", str(args.num_workers)
        ]
        if not run_script("augment_labeled_images.py", augment_args):
            logger.error("Image augmentation failed. Exiting pipeline.")
            sys.exit(1)
        logger.info("--- Image Augmentation Complete ---")
    else:
        logger.info("Skipping image augmentation step.")
        if not AUGMENTED_METADATA_OUTPUT_FILE.exists():
             logger.error(f"Skipped augmentation, but required input file {AUGMENTED_METADATA_OUTPUT_FILE} not found!")
             sys.exit(1)

    # == Step 3: Prepare Donut Dataset ==
    if not args.skip_prepare:
        logger.info("\n--- Preparing Donut Dataset ---")
        prepare_args = [
            "--aug-meta-file", str(AUGMENTED_METADATA_OUTPUT_FILE),
            "--aug-img-dir", str(AUGMENTED_IMAGE_OUTPUT_DIR),
            "--output-dir", str(DONUT_DATA_OUTPUT_DIR),
            "--test-size", str(args.val_split)
        ]
        if not run_script("prepare_donut_dataset.py", prepare_args):
            logger.error("Donut dataset preparation failed. Exiting pipeline.")
            sys.exit(1)
        logger.info("--- Donut Dataset Preparation Complete ---")
    else:
        logger.info("Skipping Donut dataset preparation step.")
        if not (DONUT_DATA_OUTPUT_DIR / "train/metadata.jsonl").exists():
             logger.error(f"Skipped preparation, but required donut data not found in {DONUT_DATA_OUTPUT_DIR}")
             sys.exit(1)

    # == Step 4: Train Donut Model ==
    if not args.skip_training:
        logger.info("\n--- Training Donut Model ---")
        train_args = [
            "--data-dir", str(DONUT_DATA_OUTPUT_DIR),
            "--output-dir", str(TRAINED_MODEL_OUTPUT_DIR),
            "--batch-size", str(args.batch_size),
            "--learning-rate", str(args.learning_rate),
            "--num-epochs", str(args.num_epochs)
            # Add other training args as needed (e.g., --from-pretrained)
        ]
        if args.resume_from_checkpoint:
            train_args.extend(["--from-pretrained", args.resume_from_checkpoint])
            
        if not run_script("train_donut.py", train_args):
            logger.error("Donut model training failed. Exiting pipeline.")
            sys.exit(1)
        logger.info("--- Donut Model Training Complete ---")
    else:
        logger.info("Skipping model training step.")

    logger.info("\nPipeline finished successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full Product Data Pipeline")

    # Step control
    parser.add_argument("--skip-labeling", action="store_true", help="Skip the GPT-4o labeling step")
    parser.add_argument("--skip-augmentation", action="store_true", help="Skip the image augmentation step")
    parser.add_argument("--skip-prepare", action="store_true", help="Skip the Donut dataset preparation step")
    parser.add_argument("--skip-training", action="store_true", help="Skip the Donut model training step")

    # Parameters
    parser.add_argument("--max-label-images", type=int, default=100, help="Max images to label with GPT-4o")
    parser.add_argument("--num-workers", type=int, default=4, help="Number of workers for parallel tasks (augmentation)")
    parser.add_argument("--val-split", type=float, default=0.1, help="Validation split size for dataset preparation")
    parser.add_argument("--batch-size", type=int, default=2, help="Batch size for training")
    parser.add_argument("--learning-rate", type=float, default=5e-5, help="Learning rate for training")
    parser.add_argument("--num-epochs", type=int, default=5, help="Number of training epochs")
    parser.add_argument("--resume-from-checkpoint", type=str, default=None, help="Path to checkpoint to resume training from")
    # Add args for GPT-4o API key if needed

    args = parser.parse_args()
    main(args)
