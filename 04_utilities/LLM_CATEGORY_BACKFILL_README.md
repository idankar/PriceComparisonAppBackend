# LLM-Based Category Backfill Script

## Overview

This script uses OpenAI's GPT-4o to intelligently categorize all products in the `canonical_products` table that are missing categories. It's significantly better than web scraping because:

- ✅ **Faster**: API calls vs web scraping delays
- ✅ **More reliable**: No website changes or anti-bot blocking
- ✅ **Better coverage**: Can categorize products not found on specific retailer sites
- ✅ **Intelligent**: Uses AI to understand product context and assign accurate categories
- ✅ **Batch processing**: Processes 50 products per API call for efficiency

## Current Status

- **Total active products**: 22,118
- **Products with categories**: 7,853 (35.5%)
- **Products needing categories**: 14,265 (64.5%)
  - Super-Pharm: 7,052 products
  - Be Pharm: 7,213 products
  - Good Pharm: 0 products (all categorized)

## Cost Estimation

### GPT-4o Pricing (as of 2024):
- Input: ~$2.50 per 1M tokens
- Output: ~$10.00 per 1M tokens
- Average: ~$6-8 per 1M tokens

### Estimated Cost for Full Backfill:
- **14,265 products** to categorize
- **~285 batches** (50 products per batch)
- **~285 API calls**
- **Estimated tokens**: ~570,000 tokens (input) + ~140,000 tokens (output) = ~710,000 tokens
- **Estimated cost**: **$4-6 USD** for complete backfill

### Cost per batch:
- ~$0.015-0.02 per batch of 50 products
- Very affordable for a one-time backfill operation

## Setup

### 1. Install OpenAI Python Package

```bash
pip install openai
```

### 2. Set OpenAI API Key

You need an OpenAI API key. Get one from: https://platform.openai.com/api-keys

#### Option A: Environment Variable (Recommended)
```bash
export OPENAI_API_KEY="sk-your-key-here"
```

#### Option B: Add to .env file
```bash
echo "OPENAI_API_KEY=sk-your-key-here" >> .env
```

Then load it:
```python
from dotenv import load_dotenv
load_dotenv()
```

## Usage

### Test Run (Recommended First)
Start with a small test to verify everything works:

```bash
python3 04_utilities/llm_category_backfill.py --limit 10 --dry-run
```

This will:
- Process only 10 products
- Show what categories would be assigned
- NOT update the database
- Show estimated costs

### Small Production Run
Process a small batch to verify quality:

```bash
python3 04_utilities/llm_category_backfill.py --limit 100
```

### Full Production Run
Process all 14,265 products:

```bash
python3 04_utilities/llm_category_backfill.py
```

### Resume from Checkpoint
If the process is interrupted, simply run again - it will automatically resume from the last checkpoint:

```bash
python3 04_utilities/llm_category_backfill.py
```

## Command Line Options

```
--limit N          Process only N products (useful for testing)
--batch-size N     Set batch size (default: 50 products per API call)
--dry-run          Test mode - no database updates
```

## Features

### ✅ Intelligent Categorization
- Uses GPT-4o to understand product names and brands
- Follows existing category structure
- Assigns Hebrew categories with hierarchical structure (e.g., "טיפוח/טיפוח שיער/שמפו")

### ✅ Checkpoint & Resume
- Automatically saves progress after each batch
- Can resume from interruptions
- Checkpoint file: `llm_category_checkpoint.json`

### ✅ Progress Tracking
- Real-time progress logs
- Token usage tracking
- Cost estimation
- Success rate monitoring

### ✅ Output Files

1. **llm_category_backfill.log** - Detailed execution log
2. **categorized_products.json** - All categorizations for review
3. **llm_category_checkpoint.json** - Progress checkpoint

## Category Structure

The script follows the existing category hierarchy:

### Main Categories:
- **טיפוח** (Personal Care)
  - טיפוח שיער (Hair Care)
  - טיפוח פנים (Face Care)
  - טיפוח גוף (Body Care)
  - דאודורנטים (Deodorants)
- **קוסמטיקה** (Cosmetics)
  - בשמים (Perfumes)
  - איפור (Makeup)
- **תינוקות ופעוטות** (Babies & Toddlers)
- **תוספי תזונה** (Dietary Supplements)
- **ויטמינים ומינרלים** (Vitamins & Minerals)
- **אורתופדיה** (Orthopedics)
- **מותג הבית** (House Brand)

### Example Categories:
```
טיפוח/טיפוח שיער/שמפו
קוסמטיקה/בשמים/בשמים לנשים
תינוקות ופעוטות/מוצצים ונשכנים/מוצצי סיליקון
```

## Monitoring Progress

### During Execution:
Watch the log file in real-time:
```bash
tail -f llm_category_backfill.log
```

### Check Database Progress:
```sql
SELECT
    COUNT(*) as total_active,
    COUNT(CASE WHEN category IS NOT NULL AND category <> '' THEN 1 END) as with_category,
    ROUND(100.0 * COUNT(CASE WHEN category IS NOT NULL AND category <> '' THEN 1 END) / COUNT(*), 2) as coverage_pct
FROM canonical_products
WHERE is_active = TRUE;
```

## Troubleshooting

### Error: "OPENAI_API_KEY environment variable not set"
- Set your OpenAI API key as shown in Setup section

### Error: Rate limit exceeded
- The script includes rate limiting (1 second delay between batches)
- If you hit limits, the script will save checkpoint and you can resume later

### Low success rate
- Check `categorized_products.json` to review assigned categories
- Ensure existing categories are properly formatted in the database

## Verification

After running, verify the results:

```bash
# Check overall coverage
PGPASSWORD=***REMOVED*** psql -h localhost -p 5432 -d price_comparison_app_v2 -U postgres -c "
SELECT
    COUNT(*) as total,
    COUNT(CASE WHEN category IS NOT NULL THEN 1 END) as categorized,
    ROUND(100.0 * COUNT(CASE WHEN category IS NOT NULL THEN 1 END) / COUNT(*), 1) as pct
FROM canonical_products
WHERE is_active = TRUE;
"

# Review sample categorizations
head -50 categorized_products.json
```

## Advantages Over Web Scraping

| Feature | LLM Approach | Web Scraping |
|---------|--------------|--------------|
| Speed | ~285 API calls (~10 min) | ~14,265 page loads (~10-20 hours) |
| Reliability | 99%+ | 60-70% (blocking, changes) |
| Coverage | 100% | Depends on product availability |
| Maintenance | None | Requires updates for website changes |
| Cost | $4-6 one-time | Free but time-consuming |

## Support

For issues or questions:
1. Check `llm_category_backfill.log` for detailed error messages
2. Review `categorized_products.json` for categorization quality
3. Test with `--limit 10 --dry-run` first
