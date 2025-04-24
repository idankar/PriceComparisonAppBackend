#!/bin/bash
# process_batches.sh
# Process products in batches with a robust approach

# First batch (1-20)
python run_batch.py --queries-file product_queries.json --start-idx 0 --end-idx 20 --batch-size 5 --delay 10 --max-pages 3

# Sleep between major batches
echo "Sleeping 2 minutes before next major batch..."
sleep 120

# Second batch (21-40)
python run_batch.py --queries-file product_queries.json --start-idx 20 --end-idx 40 --batch-size 5 --delay 10 --max-pages 3

# Sleep between major batches
echo "Sleeping 2 minutes before next major batch..."
sleep 120

# Third batch (41-60)
python run_batch.py --queries-file product_queries.json --start-idx 40 --end-idx 60 --batch-size 5 --delay 10 --max-pages 3

echo "All processing complete!"

