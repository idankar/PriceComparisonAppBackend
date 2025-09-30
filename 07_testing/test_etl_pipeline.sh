#!/bin/bash

# Test the ETL pipeline in test mode
echo "Running ETL pipeline in test mode..."
echo "This will process only 1 file per retailer and save the output to JSON files for review."
echo ""

python new_etl_pipeline.py --test

echo ""
echo "Test complete! Check the test_output/ directory for the JSON files."