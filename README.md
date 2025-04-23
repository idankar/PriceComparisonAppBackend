# Price Comparison App

A computer vision pipeline for extracting and comparing product prices.

## Directory Structure

- `config.py`: Centralized configuration
- `batch_generate_dataset.py`: Main pipeline script
- `train.py`: Model training script
- `predict.py`: Inference script
- `src/`: Source modules
- `data/`: Data storage
- `models/`: Trained models
- `logs/`: Log files

## Usage

```
python batch_generate_dataset.py --queries "נוטלה" "מילקי"
python train.py
python predict.py path/to/image.png
``` 