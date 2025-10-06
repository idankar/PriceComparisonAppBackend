# Background Scripts

This directory contains maintenance and update scripts for the PharmMate application.

## update_lowest_prices.py

Updates the `lowest_price` column in the `canonical_products` table by calculating the minimum price across all retailers for each product.

### Purpose
The API endpoints (`/api/search`, `/api/recommendations`, `/api/recommendations/popular`) read from a pre-calculated `lowest_price` column instead of computing prices in real-time. This script keeps that column up-to-date.

### Usage

**Manual Execution:**
```bash
python3 scripts/update_lowest_prices.py
```

**Scheduled Execution (Recommended):**

Set up a cron job to run every 15-30 minutes:

```bash
# Edit crontab
crontab -e

# Add this line to run every 15 minutes
*/15 * * * * cd /path/to/PriceComparisonApp && /usr/bin/python3 scripts/update_lowest_prices.py >> logs/price_updates.log 2>&1

# Or run every 30 minutes
*/30 * * * * cd /path/to/PriceComparisonApp && /usr/bin/python3 scripts/update_lowest_prices.py >> logs/price_updates.log 2>&1
```

### Output
The script logs:
- Start/completion timestamps
- Number of products updated
- Statistics (total products, min/max/avg prices)
- Any errors encountered

### Database Changes
This script modifies:
- Table: `canonical_products`
- Column: `lowest_price` (REAL)

### Performance
- Execution time: ~30-60 seconds for 30,000+ products
- No impact on API performance (runs in background)
- Updates only active products with valid prices

### Dependencies
- psycopg2
- python-dotenv
- Database credentials in `.env` file
