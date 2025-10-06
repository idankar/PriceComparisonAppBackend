# GPT-4o Price Analyzer - Usage Guide

## Overview

This tool uses OpenAI's GPT-4o to intelligently analyze the **6,407 products** with per-unit pricing and determine the correct pack price for each one.

### Why Use GPT-4o?

GPT-4o excels at:
- ✓ Understanding Hebrew product names
- ✓ Extracting pack quantities from complex text patterns
- ✓ Handling edge cases our regex patterns miss
- ✓ Providing confidence levels for each analysis
- ✓ Explaining its reasoning

---

## Setup

### 1. Get Your OpenAI API Key

**Option A: Create new account**
1. Go to https://platform.openai.com/signup
2. Sign up and verify your email
3. Add payment method at https://platform.openai.com/account/billing
4. Get API key at https://platform.openai.com/api-keys

**Option B: Use existing account**
1. Log in to https://platform.openai.com
2. Go to https://platform.openai.com/api-keys
3. Click "Create new secret key"
4. Copy the key (starts with `sk-...`)

### 2. Set Your API Key

**Option A: Environment Variable (Recommended)**
```bash
export OPENAI_API_KEY='sk-your-key-here'
```

**Option B: Pass as Parameter**
```bash
python3 gpt4_price_analyzer.py --api-key 'sk-your-key-here'
```

---

## Cost Estimation

### Per-Request Pricing (GPT-4o)
- **Input:** ~$2.50 per 1M tokens (~300 tokens per product)
- **Output:** ~$10.00 per 1M tokens (~100 tokens per response)
- **Estimated cost per product:** ~$0.001 - $0.002

### Total Cost Estimates

| Products | Estimated Cost | Estimated Time |
|----------|---------------|----------------|
| 10 (test) | ~$0.02 | 1 minute |
| 100 (sample) | ~$0.20 | 3 minutes |
| 1,000 | ~$2.00 | 25 minutes |
| 6,407 (all) | **~$12-15** | **2-3 hours** |

**Note:** Actual costs may vary. Monitor your usage at https://platform.openai.com/usage

---

## Usage

### Test Run (10 products)

Run a small test first to verify everything works:

```bash
python3 gpt4_price_analyzer.py --sample 10
```

This will:
- Analyze 10 products
- Cost: ~$0.02
- Time: ~1 minute
- Output: `gpt4_price_analysis_complete_TIMESTAMP.csv`

### Sample Run (100 products)

Test on a larger sample:

```bash
python3 gpt4_price_analyzer.py --sample 100
```

Cost: ~$0.20 | Time: ~3 minutes

### Full Analysis (6,407 products)

Run the complete analysis:

```bash
python3 gpt4_price_analyzer.py
```

Cost: ~$12-15 | Time: ~2-3 hours

**⚠️ Important:**
- The script saves progress in batches of 50
- If interrupted, you can resume (batches are saved incrementally)
- Press Ctrl+C to cancel at any time

---

## Advanced Options

### Adjust Batch Size
```bash
python3 gpt4_price_analyzer.py --batch-size 100
```
Larger batches = faster but higher risk if interrupted

### Adjust Request Delay
```bash
python3 gpt4_price_analyzer.py --delay 0.5
```
Lower delay = faster but may hit rate limits

### Combined Example
```bash
python3 gpt4_price_analyzer.py \
    --sample 500 \
    --batch-size 100 \
    --delay 0.5
```

---

## Output Files

### 1. Batch Files (Incremental Saves)
```
gpt4_analysis_batch_1.csv
gpt4_analysis_batch_2.csv
...
```
Each batch is saved as it completes, so progress isn't lost.

### 2. Complete Analysis
```
gpt4_price_analysis_complete_20251006_153045.csv
```
Contains all analyzed products with:
- `pack_quantity` - Detected pack size
- `unit_type` - Type of unit (capsule, wipe, meter, etc.)
- `normalized_price` - Calculated pack price
- `calculation` - Explanation of how price was calculated
- `confidence` - high/medium/low
- `notes` - Additional observations

### 3. High-Confidence Normalizations
```
gpt4_high_confidence_normalizations_20251006_153045.csv
```
Filtered to only products GPT-4o is confident about - ready to apply directly to database.

---

## Expected Results

Based on the patterns we've seen:

### High Confidence Expected (~60-70%)
Products with clear patterns:
- "מארז 3" → 3-pack
- "150 יחידות" → 150 units
- "100 קפסולות" → 100 capsules
- "זוג" → 2-pack

### Medium Confidence (~20-30%)
Products with ambiguous text:
- Implied quantities
- Multiple possible interpretations
- Missing explicit numbers

### Low Confidence (~5-10%)
Products where GPT-4o cannot determine:
- Very vague product names
- Missing quantity information
- Contradictory information

---

## After Analysis

### 1. Review Results
```bash
# View high-confidence results
head -20 gpt4_high_confidence_normalizations_*.csv

# Count by confidence
python3 -c "
import pandas as pd
df = pd.read_csv('gpt4_price_analysis_complete_*.csv')
print(df['confidence'].value_counts())
"
```

### 2. Apply High-Confidence Normalizations

I can create a script to:
1. Load the high-confidence results
2. Apply those normalizations to the database
3. Keep medium/low confidence for manual review

### 3. Manual Review

For medium/low confidence items:
- Review the `calculation` and `notes` columns
- Manually verify a sample
- Decide which to apply

---

## Troubleshooting

### "OpenAI API key not found"
- Check your environment variable: `echo $OPENAI_API_KEY`
- Or pass explicitly: `--api-key sk-your-key`

### "Rate limit exceeded"
- Increase delay: `--delay 2.0`
- Reduce batch size: `--batch-size 20`

### "Insufficient quota"
- Add credits at https://platform.openai.com/account/billing
- Start with a smaller sample: `--sample 100`

### Script Interrupted
- Batch files are saved incrementally
- Check `gpt4_analysis_batch_*.csv` files
- Can manually combine them if needed

---

## Example Output

```json
{
  "pack_quantity": 150,
  "unit_type": "capsule",
  "normalized_price": 60.00,
  "calculation": "0.40 (per capsule) × 150 (capsules) = 60.00",
  "confidence": "high",
  "notes": "Product name clearly states '150 קפסולות' (150 capsules)"
}
```

---

## Next Steps

1. **Test first:** Run with `--sample 10`
2. **Review test output:** Check if results make sense
3. **Scale up:** Try `--sample 100`
4. **Full analysis:** Run on all 6,407 products (~$12-15)
5. **Apply results:** Use high-confidence normalizations

---

## Cost Optimization Tips

1. **Start small:** Test with 10-100 products first
2. **Use batches:** Script already implements this
3. **Monitor usage:** Check https://platform.openai.com/usage
4. **Set limits:** Set spending limits in OpenAI dashboard

---

## Questions?

The script is ready to run. You can:
- Start with a test: `python3 gpt4_price_analyzer.py --sample 10`
- Review the test results
- Scale up as comfortable

**Estimated total cost for all 6,407 products: $12-15**
