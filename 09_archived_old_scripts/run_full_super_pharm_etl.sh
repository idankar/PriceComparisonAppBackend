#!/bin/bash
# Full Super-Pharm ETL Script - Process ALL stores and price files

echo "========================================"
echo "FULL SUPER-PHARM TRANSPARENCY ETL"
echo "Processing ALL 2,177 files"
echo "Expected: ~180 stores, ~2.8M price points"
echo "========================================"

# Run without limits to get all files
/Users/noa/Desktop/PriceComparisonApp/fresh_env/bin/python \
    01_data_scraping_pipeline/super_pharm_transparency_etl.py \
    2>&1 | tee super_pharm_full_etl_complete.log

echo "ETL Complete! Check super_pharm_full_etl_complete.log for details"