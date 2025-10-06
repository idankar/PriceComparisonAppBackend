"""
GPT-4o Price Analyzer
=====================
This script uses OpenAI's GPT-4o to analyze per-unit pricing outliers and determine
the correct pack price calculation for each product.

GPT-4o is excellent at:
- Understanding Hebrew product names
- Extracting pack quantities from complex text
- Determining pricing units (per capsule, per meter, per wipe, etc.)
- Calculating correct pack prices

Requirements:
- pip install openai
- OpenAI API key set in environment or passed as parameter
"""

import psycopg2
import pandas as pd
import json
import os
import time
from datetime import datetime
from typing import Optional, List, Dict
import re

try:
    from openai import OpenAI
except ImportError:
    print("ERROR: OpenAI package not installed.")
    print("Install with: pip install openai")
    exit(1)

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

# GPT-4o analysis prompt
ANALYSIS_PROMPT = """You are a pricing expert analyzing Israeli pharmacy/retail product names to determine correct pack prices.

CRITICAL RULES:
1. VOLUME measurements (ml, מל, ליטר, liter) are NOT pack quantities - they describe bottle/container size
2. WEIGHT measurements (gram, גרם, קילו, kg) are NOT pack quantities - they describe product weight
3. ONLY count actual discrete items: capsules, wipes, diapers, tablets, units, packs

Product Information:
- Original Name: {original_name}
- Canonical Name: {canonical_name}
- Current Price: ₪{current_price}

EXAMPLES OF CORRECT INTERPRETATION:

✓ CORRECT:
"3 יחידות" → pack_quantity: 3, unit_type: "unit"
"מארז 3" → pack_quantity: 3, unit_type: "pack"
"150 קפסולות" → pack_quantity: 150, unit_type: "capsule"
"זוג" → pack_quantity: 2, unit_type: "pair"
"40 מגבונים" → pack_quantity: 40, unit_type: "wipe"

✗ WRONG (DO NOT DO THIS):
"700 מל" → pack_quantity: 700 ❌ (This is VOLUME, not quantity!)
"500 ml" → pack_quantity: 500 ❌ (This is VOLUME, not quantity!)
"280 גרם" → pack_quantity: 280 ❌ (This is WEIGHT, not quantity!)
"1 ליטר" → pack_quantity: 1000 ❌ (This is VOLUME, not quantity!)

CORRECT INTERPRETATION FOR VOLUME/WEIGHT PRODUCTS:
"700 מל" → pack_quantity: 1, unit_type: "bottle" (ONE 700ml bottle)
"500 ml" → pack_quantity: 1, unit_type: "bottle" (ONE 500ml bottle)
"280 גרם" → pack_quantity: 1, unit_type: "bar" or "package" (ONE 280g item)

PATTERNS TO RECOGNIZE:

Pack Indicators (USE THESE):
- "מארז X" = pack of X
- "X יחידות" = X units
- "X קפסולות" / "X capsules" = X capsules
- "X טבליות" / "X tablets" = X tablets
- "X כמוסות" = X capsules
- "X מגבונים" = X wipes
- "X חיתולים" = X diapers
- "זוג" = 2 items
- "שלישייה" = 3 items
- "X יח'" / "X יח" = X units

Volume/Weight Indicators (IGNORE FOR PACK QUANTITY):
- "X מל" / "X ml" / "X מ"ל" = volume in milliliters (NOT pack quantity!)
- "X ליטר" / "X liter" / "X L" = volume in liters (NOT pack quantity!)
- "X גרם" / "X gram" / "X gr" = weight in grams (NOT pack quantity!)
- "X קילו" / "X kg" = weight in kilograms (NOT pack quantity!)

Per-Unit Pricing Indicators:
- "(₪X ליחידה)" = price is per individual unit
- "(₪X ל-1 מטר)" = price is per meter
- "(₪X לקפסולה)" = price is per capsule

ANALYSIS APPROACH:

1. Check if product name contains volume/weight measurements:
   - If YES and no other pack indicators → pack_quantity: 1, unit_type: "bottle"/"package", confidence: "high"

2. Look for explicit pack quantity indicators:
   - "מארז 3", "150 יחידות", "זוג", etc. → Use the explicit quantity, confidence: "high"

3. If NO volume/weight AND NO explicit pack indicators:
   - DEFAULT: pack_quantity: 1, unit_type: "package"
   - Confidence: "medium" (reasonable assumption for retail products)
   - ONLY use confidence: "low" if the product name is truly ambiguous or incomplete

4. Calculate normalized_price:
   - Always: current_price × pack_quantity

Respond in JSON format only:
{{
    "pack_quantity": <number>,
    "unit_type": "<capsule|unit|meter|wipe|tablet|bottle|package|bar|pair|etc>",
    "normalized_price": <calculated_price>,
    "calculation": "<explanation of your reasoning>",
    "confidence": "<high|medium|low>",
    "notes": "<any additional observations>"
}}

CONFIDENCE GUIDELINES:
- HIGH: Explicit pack indicators (יחידות, מארז, זוג) OR volume/weight measurements present
- MEDIUM: No explicit indicators, but reasonable to assume single item based on product type
- LOW: Product name is incomplete, truncated, or truly ambiguous (use sparingly!)

IMPORTANT:
- ALWAYS provide a pack_quantity (never null)
- If the product is a VOLUME-based item (700ml, 500ml, etc.) → pack_quantity: 1, unit_type: "bottle", confidence: "high"
- If the product is a WEIGHT-based item (280g, 500g, etc.) → pack_quantity: 1, unit_type: "package" or "bar", confidence: "high"
- For items without indicators (candies, masks, deodorants) → pack_quantity: 1, unit_type: "package", confidence: "medium"
- Consistency is critical: similar products should get similar confidence ratings
"""

def get_per_unit_products():
    """Fetch all Super-Pharm products with per-unit pricing."""
    query = """
    WITH latest_prices AS (
        SELECT DISTINCT ON (rp.retailer_product_id)
            rp.retailer_product_id,
            rp.barcode,
            rp.original_retailer_name,
            cp.name as canonical_name,
            cp.category,
            p.price,
            p.store_id
        FROM prices p
        JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
        JOIN canonical_products cp ON rp.barcode = cp.barcode
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE r.retailername = 'Super-Pharm'
        ORDER BY rp.retailer_product_id, p.price_timestamp DESC
    )
    SELECT
        retailer_product_id,
        barcode,
        original_retailer_name,
        canonical_name,
        category,
        price,
        store_id
    FROM latest_prices
    WHERE original_retailer_name LIKE '%(₪%ל%'
        OR original_retailer_name LIKE '%ליחידה%'
        OR canonical_name LIKE '%(₪%ל%'
        OR canonical_name LIKE '%ליחידה%';
    """

    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df

def analyze_product_with_gpt4(client: OpenAI, product: Dict, model: str = "gpt-4o") -> Dict:
    """
    Analyze a single product using GPT-4o.
    """
    prompt = ANALYSIS_PROMPT.format(
        original_name=product['original_retailer_name'],
        canonical_name=product['canonical_name'],
        current_price=product['price']
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a pricing expert specializing in Israeli pharmacy and retail products. You understand Hebrew product names and pricing patterns."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,  # Low temperature for consistent results
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        # Add original product info
        result['retailer_product_id'] = product['retailer_product_id']
        result['barcode'] = product['barcode']
        result['original_name'] = product['original_retailer_name']
        result['canonical_name'] = product['canonical_name']
        result['current_price'] = product['price']
        result['category'] = product['category']
        result['store_id'] = product['store_id']

        return result

    except Exception as e:
        return {
            'retailer_product_id': product['retailer_product_id'],
            'barcode': product['barcode'],
            'original_name': product['original_retailer_name'],
            'canonical_name': product['canonical_name'],
            'current_price': product['price'],
            'category': product['category'],
            'store_id': product['store_id'],
            'pack_quantity': None,
            'unit_type': None,
            'normalized_price': None,
            'calculation': None,
            'confidence': 'error',
            'notes': f'Error: {str(e)}'
        }

def analyze_batch(client: OpenAI, products: List[Dict], batch_size: int = 50,
                 delay: float = 1.0, progress_callback=None) -> List[Dict]:
    """
    Analyze products in batches to avoid rate limits.
    """
    results = []
    total = len(products)

    for i in range(0, total, batch_size):
        batch = products[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"\n[Batch {batch_num}/{total_batches}] Processing {len(batch)} products...")

        batch_results = []
        for j, product in enumerate(batch):
            result = analyze_product_with_gpt4(client, product)
            batch_results.append(result)
            results.append(result)

            # Progress within batch
            if (j + 1) % 10 == 0:
                print(f"  Progress: {j + 1}/{len(batch)}")

            # Rate limiting
            if j < len(batch) - 1:  # Don't delay after last item
                time.sleep(delay)

        # Save batch results incrementally
        batch_df = pd.DataFrame(batch_results)
        batch_file = f'gpt4_analysis_batch_{batch_num}.csv'
        batch_df.to_csv(batch_file, index=False, encoding='utf-8-sig')
        print(f"  ✓ Batch {batch_num} saved to {batch_file}")

        # Callback for progress tracking
        if progress_callback:
            progress_callback(i + len(batch), total)

        # Longer delay between batches
        if i + batch_size < total:
            print(f"  Waiting {delay * 2:.1f}s before next batch...")
            time.sleep(delay * 2)

    return results

def main(api_key: Optional[str] = None, sample_size: Optional[int] = None,
         batch_size: int = 50, delay: float = 1.0, no_confirm: bool = False):
    """
    Main execution function.
    """
    print("=" * 80)
    print("GPT-4O PRICE ANALYZER")
    print("=" * 80)

    # Get API key
    if not api_key:
        api_key = os.getenv('OPENAI_API_KEY')

    if not api_key:
        print("\n❌ ERROR: OpenAI API key not found")
        print("\nPlease provide API key via:")
        print("  1. Environment variable: export OPENAI_API_KEY='your-key'")
        print("  2. Script parameter: main(api_key='your-key')")
        print("\nGet your API key from: https://platform.openai.com/api-keys")
        return

    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)

    print("\n[1/4] Fetching products with per-unit pricing...")
    df = get_per_unit_products()
    print(f"      ✓ Found {len(df):,} products")

    # Sample if requested
    if sample_size:
        print(f"\n⚠️  Using sample of {sample_size} products for testing")
        df = df.head(sample_size)

    products = df.to_dict('records')

    print(f"\n[2/4] Analyzing {len(products):,} products with GPT-4o...")
    print(f"      Batch size: {batch_size}")
    print(f"      Delay between requests: {delay}s")

    # Estimate time and cost
    estimated_time = (len(products) * delay + (len(products) // batch_size) * delay * 2) / 60
    estimated_cost = len(products) * 0.01  # Rough estimate: $0.01 per request

    print(f"\n      Estimated time: {estimated_time:.1f} minutes")
    print(f"      Estimated cost: ${estimated_cost:.2f} (at ~$0.01 per request)")

    if not no_confirm:
        input(f"\n      Press Enter to continue or Ctrl+C to cancel...")
    else:
        print(f"\n      Auto-starting analysis (--no-confirm flag set)...")

    start_time = time.time()

    results = analyze_batch(client, products, batch_size=batch_size, delay=delay)

    elapsed_time = (time.time() - start_time) / 60

    print(f"\n[3/4] Analysis complete in {elapsed_time:.1f} minutes")

    # Create results DataFrame
    results_df = pd.DataFrame(results)

    # Statistics
    print("\n[4/4] Analyzing results...")

    total = len(results_df)
    with_pack_qty = results_df[results_df['pack_quantity'].notna()]
    high_conf = results_df[results_df['confidence'] == 'high']
    medium_conf = results_df[results_df['confidence'] == 'medium']
    low_conf = results_df[results_df['confidence'] == 'low']
    errors = results_df[results_df['confidence'] == 'error']

    print(f"\n      Total analyzed: {total:,}")
    print(f"      Pack quantity determined: {len(with_pack_qty):,} ({len(with_pack_qty)/total*100:.1f}%)")
    print(f"\n      Confidence breakdown:")
    print(f"        High: {len(high_conf):,} ({len(high_conf)/total*100:.1f}%)")
    print(f"        Medium: {len(medium_conf):,} ({len(medium_conf)/total*100:.1f}%)")
    print(f"        Low: {len(low_conf):,} ({len(low_conf)/total*100:.1f}%)")
    print(f"        Errors: {len(errors):,} ({len(errors)/total*100:.1f}%)")

    # Save complete results
    output_file = f'gpt4_price_analysis_complete_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n      ✓ Results saved to: {output_file}")

    # Save high-confidence normalizations separately
    high_conf_file = f'gpt4_high_confidence_normalizations_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    high_conf_normalized = results_df[
        (results_df['confidence'] == 'high') &
        (results_df['normalized_price'].notna())
    ]
    high_conf_normalized.to_csv(high_conf_file, index=False, encoding='utf-8-sig')
    print(f"      ✓ High-confidence normalizations: {high_conf_file}")
    print(f"        ({len(high_conf_normalized):,} products ready for normalization)")

    # Summary statistics
    if len(with_pack_qty) > 0:
        print(f"\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)
        print(f"\nPack quantity distribution:")
        print(with_pack_qty['pack_quantity'].value_counts().head(10))

        print(f"\nUnit type distribution:")
        print(results_df['unit_type'].value_counts().head(10))

        print(f"\nPrice normalization:")
        normalizable = results_df[results_df['normalized_price'].notna()]
        if len(normalizable) > 0:
            print(f"  Current price range: ₪{normalizable['current_price'].min():.2f} - ₪{normalizable['current_price'].max():.2f}")
            print(f"  Normalized price range: ₪{normalizable['normalized_price'].min():.2f} - ₪{normalizable['normalized_price'].max():.2f}")

    print("\n" + "=" * 80)

    return results_df

if __name__ == "__main__":
    import sys

    # Parse command line arguments
    api_key = None
    sample_size = None
    batch_size = 50
    delay = 1.0
    no_confirm = False

    # Check for API key in arguments
    if '--api-key' in sys.argv:
        idx = sys.argv.index('--api-key')
        if idx + 1 < len(sys.argv):
            api_key = sys.argv[idx + 1]

    # Check for sample size
    if '--sample' in sys.argv:
        idx = sys.argv.index('--sample')
        if idx + 1 < len(sys.argv):
            sample_size = int(sys.argv[idx + 1])

    # Check for batch size
    if '--batch-size' in sys.argv:
        idx = sys.argv.index('--batch-size')
        if idx + 1 < len(sys.argv):
            batch_size = int(sys.argv[idx + 1])

    # Check for delay
    if '--delay' in sys.argv:
        idx = sys.argv.index('--delay')
        if idx + 1 < len(sys.argv):
            delay = float(sys.argv[idx + 1])

    # Check for no-confirm flag
    if '--no-confirm' in sys.argv:
        no_confirm = True

    # Run analysis
    results = main(
        api_key=api_key,
        sample_size=sample_size,
        batch_size=batch_size,
        delay=delay,
        no_confirm=no_confirm
    )
