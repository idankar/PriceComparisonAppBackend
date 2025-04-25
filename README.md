# Price Comparison App

A computer vision-powered application for extracting and comparing product prices across Israeli supermarkets.

## Directory Structure

```
PriceComparisonApp/
├── src/                            # Core source code
│   ├── api/                        # API integrations
│   ├── models/                     # ML models
│   ├── utils/                      # Utility functions
│   └── scrapers/                   # Web scrapers
├── data/                           # Data directory
│   ├── images/                     # Product images
│   │   ├── raw/                    # Original images
│   │   └── augmented/              # Augmented images
│   ├── database/                   # Database files
│   └── results/                    # Extracted results
│       ├── csv/                    # CSV result files
│       └── json/                   # JSON result files
├── notebooks/                      # Jupyter notebooks for exploration
├── scripts/                        # Executable scripts
├── archive/                        # Archive of old approaches
└── tests/                          # Test cases
```

## Components

- **Data Collection**: Fetch product data from Shufersal API
- **Database**: SQLite-based product database with image and text embeddings
- **Search**: Search for products by image or text query
- **Price Comparison**: Compare prices across different stores

## Usage

### Collecting Product Data

```bash
# Collect data for a single query
python scripts/price_comparison.py collect --query "חלב"

# Collect data for multiple queries from a file
python scripts/price_comparison.py collect --query-file product_queries.txt --max-pages 3
```

### Building the Database

```bash
# Build database from collected data
python scripts/price_comparison.py build --data-dir data/results

# Build with auto-translation for product names
python scripts/price_comparison.py build --auto-translate
```

### Searching for Products

```bash
# Search by image
python scripts/price_comparison.py search --image data/images/my_product.jpg

# Search by text (works in Hebrew or English)
python scripts/price_comparison.py search --text "חלב תנובה" --limit 10
```

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 