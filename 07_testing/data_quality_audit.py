"""
Data Quality Audit Script
===========================
This script audits the product database to identify and flag critical pricing errors.
It implements three detection rules:
1. Statistical Outlier Detection (>3x median price)
2. Unit Price Detection (keyword-based + magnitude-based)
3. Unreasonably Low Price Detection (<₪10 for non-category products)

Output: data_quality_audit.csv
"""

import psycopg2
import pandas as pd
import numpy as np
from typing import List, Dict
import re

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

# Detection parameters
OUTLIER_MULTIPLIER = 3.0  # Flag prices >3x median
UNIT_PRICE_KEYWORDS = [
    'ליחידה', 'per unit', 'לקפסולה', 'per capsule',
    '100 גרם', '100g', "ליח'", 'ליח"', 'ליח`'
]
MULTIPACK_CATEGORIES = [
    'Vitamins & Supplements', 'Wipes', 'Diapers', 'Cleaning Supplies'
]
MULTIPACK_THRESHOLD = 5.00  # ₪5.00
ABSOLUTE_FLOOR = 10.00  # ₪10.00

def get_latest_prices_with_metadata() -> pd.DataFrame:
    """
    Fetch the latest price for each retailer_product_id along with product metadata.
    Returns a DataFrame with columns: barcode, product_name, retailer_name, price, category
    """
    query = """
    WITH latest_prices AS (
        SELECT DISTINCT ON (retailer_product_id)
            retailer_product_id,
            price,
            price_timestamp
        FROM prices
        ORDER BY retailer_product_id, price_timestamp DESC
    )
    SELECT
        cp.barcode,
        cp.name AS product_name,
        r.retailername AS retailer_name,
        lp.price,
        cp.category,
        rp.retailer_product_id
    FROM latest_prices lp
    JOIN retailer_products rp ON lp.retailer_product_id = rp.retailer_product_id
    JOIN canonical_products cp ON rp.barcode = cp.barcode
    JOIN retailers r ON rp.retailer_id = r.retailerid
    WHERE lp.price IS NOT NULL
        AND lp.price > 0
        AND cp.barcode IS NOT NULL
    ORDER BY cp.barcode, r.retailername;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df

def calculate_median_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate median price for each barcode across all retailers.
    """
    median_prices = df.groupby('barcode')['price'].median().reset_index()
    median_prices.columns = ['barcode', 'median_price']
    return median_prices

def detect_outliers(df: pd.DataFrame, median_prices: pd.DataFrame) -> List[Dict]:
    """
    Rule #1: Statistical Outlier Detection
    Flag prices that are more than 3x the median price for the barcode.
    """
    flagged = []
    df_with_median = df.merge(median_prices, on='barcode', how='left')

    for _, row in df_with_median.iterrows():
        if row['price'] > (row['median_price'] * OUTLIER_MULTIPLIER):
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'reason_for_flag': 'Potential Pricing Error'
            })

    return flagged

def detect_unit_price_keywords(df: pd.DataFrame, median_prices: pd.DataFrame) -> List[Dict]:
    """
    Rule #2A: Unit Price Detection - Keyword-based
    Flag products whose name contains unit pricing keywords.
    """
    flagged = []
    df_with_median = df.merge(median_prices, on='barcode', how='left')

    # Create regex pattern for all keywords
    pattern = '|'.join(re.escape(keyword) for keyword in UNIT_PRICE_KEYWORDS)

    for _, row in df_with_median.iterrows():
        product_name = str(row['product_name']).lower()
        if re.search(pattern, product_name, re.IGNORECASE):
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'reason_for_flag': 'Suspected Unit Price (Keyword)'
            })

    return flagged

def detect_unit_price_magnitude(df: pd.DataFrame, median_prices: pd.DataFrame) -> List[Dict]:
    """
    Rule #2B: Unit Price Detection - Magnitude-based
    Flag products in multipack categories with prices below ₪5.00.
    """
    flagged = []
    df_with_median = df.merge(median_prices, on='barcode', how='left')

    for _, row in df_with_median.iterrows():
        category = str(row['category']) if pd.notna(row['category']) else ''
        if category in MULTIPACK_CATEGORIES and row['price'] < MULTIPACK_THRESHOLD:
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'reason_for_flag': 'Suspected Unit Price (Magnitude)'
            })

    return flagged

def detect_unreasonably_low_prices(df: pd.DataFrame, median_prices: pd.DataFrame) -> List[Dict]:
    """
    Rule #3: Unreasonably Low Price Detection
    Flag products not in multipack categories with prices below ₪10.00.
    """
    flagged = []
    df_with_median = df.merge(median_prices, on='barcode', how='left')

    for _, row in df_with_median.iterrows():
        category = str(row['category']) if pd.notna(row['category']) else ''
        if category not in MULTIPACK_CATEGORIES and row['price'] < ABSOLUTE_FLOOR:
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'reason_for_flag': 'Suspiciously Low Price'
            })

    return flagged

def main():
    """
    Main execution function.
    """
    print("=" * 80)
    print("DATA QUALITY AUDIT - PRICING ERROR DETECTION")
    print("=" * 80)

    # Step 1: Fetch data
    print("\n[1/6] Fetching latest prices with metadata from database...")
    df = get_latest_prices_with_metadata()
    print(f"      ✓ Loaded {len(df):,} price records")
    print(f"      ✓ Covering {df['barcode'].nunique():,} unique products")
    print(f"      ✓ From {df['retailer_name'].nunique()} retailers")

    # Step 2: Calculate median prices
    print("\n[2/6] Calculating median prices for each barcode...")
    median_prices = calculate_median_prices(df)
    print(f"      ✓ Calculated median for {len(median_prices):,} barcodes")

    # Step 3: Rule #1 - Statistical Outliers
    print("\n[3/6] Applying Rule #1: Statistical Outlier Detection (>3x median)...")
    outliers = detect_outliers(df, median_prices)
    print(f"      ✓ Flagged {len(outliers):,} outlier prices")

    # Step 4: Rule #2A - Keyword Detection
    print("\n[4/6] Applying Rule #2A: Unit Price Keyword Detection...")
    keyword_flags = detect_unit_price_keywords(df, median_prices)
    print(f"      ✓ Flagged {len(keyword_flags):,} unit price keywords")

    # Step 5: Rule #2B - Magnitude Detection
    print("\n[5/6] Applying Rule #2B: Unit Price Magnitude Detection (<₪5.00)...")
    magnitude_flags = detect_unit_price_magnitude(df, median_prices)
    print(f"      ✓ Flagged {len(magnitude_flags):,} magnitude-based unit prices")

    # Step 6: Rule #3 - Unreasonably Low Prices
    print("\n[6/6] Applying Rule #3: Unreasonably Low Price Detection (<₪10.00)...")
    low_price_flags = detect_unreasonably_low_prices(df, median_prices)
    print(f"      ✓ Flagged {len(low_price_flags):,} unreasonably low prices")

    # Combine all flags
    all_flags = outliers + keyword_flags + magnitude_flags + low_price_flags

    # Create output DataFrame
    output_df = pd.DataFrame(all_flags)

    # Remove duplicates (a price might be flagged by multiple rules)
    # Keep the first occurrence
    output_df = output_df.drop_duplicates(
        subset=['barcode', 'retailer_name', 'suspicious_price'],
        keep='first'
    )

    # Sort by barcode and suspicious price
    output_df = output_df.sort_values(['barcode', 'suspicious_price'])

    # Write to CSV
    output_file = 'data_quality_audit.csv'
    output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
    print(f"\nTotal Flagged Prices: {len(output_df):,}")
    print(f"\nBreakdown by Flag Reason:")
    for reason, count in output_df['reason_for_flag'].value_counts().items():
        print(f"  • {reason}: {count:,}")
    print(f"\nOutput saved to: {output_file}")
    print("=" * 80)

if __name__ == "__main__":
    main()
