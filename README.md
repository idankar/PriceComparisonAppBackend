# Price Comparison App

A computer vision pipeline for extracting and comparing product prices.

## Directory Structure

- `config.py`: Centralized configuration (Now in `src/`)
- `batch_generate_dataset.py`: Main pipeline script
- `train.py`: Model training script
- `predict.py`: Inference script
- `src/`: Source modules (contains `config.py`, `ocr.py`, `dataset.py`, etc.)
- `data/`: Data storage
- `models/`: Trained models
- `logs/`: Log files

## Usage

```
# Example: Run pipeline for specific queries
python batch_generate_dataset.py --queries "נוטלה" "מילקי"

# Train the Donut model
python train.py

# Predict on a single image
python predict.py path/to/image.png
```
