"""
Data Quality Audit Script - FINAL VERSION
==========================================
This script audits the product database to identify genuine pricing errors,
excluding Super-Pharm's informational per-unit pricing displays.

Key Changes from Previous Version:
- Excludes products where original_retailer_name contains per-unit pricing
- Focuses only on TRUE pricing errors and statistical outliers
- No longer flags Super-Pharm's per-unit/per-meter informational pricing

Output: data_quality_audit_final.csv
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

# Patterns that indicate informational pricing (not actual pricing errors)
INFORMATIONAL_PRICING_PATTERNS = [
    r'\(₪[\d,.]+ ל-',  # (₪0.24 ל-)
    r'\(₪[\d,.]+ ליחידה',  # (₪3.74 ליחידה)
    r'\(₪[\d,.]+ לקפסולה',  # (₪2.00 לקפסולה)
    r'ל-1 מ"ר',  # per square meter
    r'ל-1 מטר',  # per meter
    r'למטר',  # per meter
    r'ליח\'',  # per unit
    r'ליח"',  # per unit
    r'ליח`',  # per unit
]

def has_informational_pricing(text: str) -> bool:
    """Check if text contains informational per-unit pricing indicators."""
    if pd.isna(text):
        return False
    for pattern in INFORMATIONAL_PRICING_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False

def get_latest_prices_with_metadata() -> pd.DataFrame:
    """
    Fetch the latest price for each retailer_product_id along with product metadata
    and original retailer names.
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
        rp.retailer_product_id,
        rp.original_retailer_name
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

    # Mark products with informational pricing
    df['has_informational_pricing'] = df.apply(
        lambda row: has_informational_pricing(row['product_name']) or
                   has_informational_pricing(row['original_retailer_name']),
        axis=1
    )

    return df

def calculate_price_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate price statistics for each barcode across all retailers,
    EXCLUDING prices with informational pricing.
    """
    # Filter out informational pricing entries
    clean_df = df[~df['has_informational_pricing']].copy()

    if len(clean_df) == 0:
        return pd.DataFrame(columns=['barcode', 'median_price', 'min_price', 'max_price', 'retailer_count', 'price_ratio'])

    stats = clean_df.groupby('barcode').agg({
        'price': ['median', 'min', 'max', 'count']
    }).reset_index()

    stats.columns = ['barcode', 'median_price', 'min_price', 'max_price', 'retailer_count']
    stats['price_ratio'] = stats['max_price'] / stats['min_price'].replace(0, np.nan)

    return stats

def detect_outliers(df: pd.DataFrame, price_stats: pd.DataFrame) -> List[Dict]:
    """
    Rule #1: Statistical Outlier Detection
    Flag prices that are more than 3x the median price for the barcode.
    Exclude prices with informational pricing.
    """
    flagged = []

    # Only check prices WITHOUT informational pricing
    clean_df = df[~df['has_informational_pricing']].copy()
    df_with_stats = clean_df.merge(price_stats, on='barcode', how='left')

    for _, row in df_with_stats.iterrows():
        if pd.isna(row['median_price']):
            continue

        if row['price'] > (row['median_price'] * OUTLIER_MULTIPLIER):
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'price_ratio': float(row['price'] / row['median_price']),
                'retailer_count': int(row['retailer_count']) if not pd.isna(row['retailer_count']) else 0,
                'reason_for_flag': 'Statistical Outlier (>3x median price)'
            })

    return flagged

def detect_suspiciously_low_prices(df: pd.DataFrame, price_stats: pd.DataFrame) -> List[Dict]:
    """
    Rule #2: Suspiciously Low Prices for High-Value Categories
    Flag prices that are unreasonably low (<₪1) for pharmacy/health products.
    This catches data entry errors or incorrect unit conversions.
    """
    flagged = []

    # Only check prices WITHOUT informational pricing
    clean_df = df[~df['has_informational_pricing']].copy()
    df_with_stats = clean_df.merge(price_stats, on='barcode', how='left')

    HIGH_VALUE_CATEGORIES = [
        'בית מרקחת', 'טבע וויטמינים', 'בריאות'
    ]

    SUSPICIOUS_THRESHOLD = 1.00  # ₪1.00

    for _, row in df_with_stats.iterrows():
        category = str(row['category']) if pd.notna(row['category']) else ''

        # Check if it's a high-value category
        is_high_value = any(hv_cat in category for hv_cat in HIGH_VALUE_CATEGORIES)

        if is_high_value and row['price'] < SUSPICIOUS_THRESHOLD:
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']) if not pd.isna(row['median_price']) else 0,
                'price_ratio': float(row['price'] / row['median_price']) if (not pd.isna(row['median_price']) and row['median_price'] > 0) else 0,
                'retailer_count': int(row['retailer_count']) if not pd.isna(row['retailer_count']) else 0,
                'reason_for_flag': f'Suspiciously Low Price for High-Value Category ({category})'
            })

    return flagged

def main():
    """
    Main execution function.
    """
    print("=" * 80)
    print("DATA QUALITY AUDIT - FINAL VERSION")
    print("=" * 80)
    print("\nThis version excludes Super-Pharm's informational per-unit pricing")
    print("and focuses on genuine pricing errors only.")

    # Step 1: Fetch data
    print("\n[1/4] Fetching latest prices with metadata from database...")
    df = get_latest_prices_with_metadata()
    print(f"      ✓ Loaded {len(df):,} price records")
    print(f"      ✓ Covering {df['barcode'].nunique():,} unique products")
    print(f"      ✓ From {df['retailer_name'].nunique()} retailers")

    informational_count = df['has_informational_pricing'].sum()
    print(f"      ✓ {informational_count:,} prices have informational pricing (will be excluded)")

    # Step 2: Calculate price statistics (excluding informational pricing)
    print("\n[2/4] Calculating price statistics (excluding informational pricing)...")
    price_stats = calculate_price_stats(df)
    print(f"      ✓ Calculated stats for {len(price_stats):,} barcodes")

    # Step 3: Rule #1 - Statistical Outliers
    print("\n[3/4] Applying Rule #1: Statistical Outlier Detection (>3x median)...")
    outliers = detect_outliers(df, price_stats)
    print(f"      ✓ Flagged {len(outliers):,} statistical outliers")

    # Step 4: Rule #2 - Suspiciously Low Prices
    print("\n[4/4] Applying Rule #2: Suspiciously Low Prices (<₪1 for health products)...")
    low_price_flags = detect_suspiciously_low_prices(df, price_stats)
    print(f"      ✓ Flagged {len(low_price_flags):,} suspiciously low prices")

    # Combine all flags
    all_flags = outliers + low_price_flags

    # Create output DataFrame
    output_df = pd.DataFrame(all_flags)

    if len(output_df) > 0:
        # Remove duplicates
        output_df = output_df.drop_duplicates(
            subset=['barcode', 'retailer_name', 'suspicious_price'],
            keep='first'
        )

        # Sort by price_ratio (most suspicious first)
        output_df = output_df.sort_values(['price_ratio'], ascending=False)

        # Reorder columns
        output_df = output_df[[
            'barcode', 'product_name', 'retailer_name', 'suspicious_price',
            'median_price_for_barcode', 'price_ratio', 'retailer_count', 'reason_for_flag'
        ]]

    # Write to CSV
    output_file = 'data_quality_audit_final.csv'
    output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
    print(f"\nTotal Flagged Prices: {len(output_df):,}")

    if len(output_df) > 0:
        print(f"\nTop 10 Issues by Price Ratio:")
        top_10 = output_df.head(10)
        for idx, row in top_10.iterrows():
            print(f"  • {row['product_name'][:60]}")
            print(f"    ₪{row['suspicious_price']:.2f} vs median ₪{row['median_price_for_barcode']:.2f} ({row['price_ratio']:.1f}x) at {row['retailer_name']}")

        print(f"\nBreakdown by Flag Reason:")
        for reason, count in output_df['reason_for_flag'].value_counts().items():
            print(f"  • {reason}: {count:,}")
    else:
        print("\n✓ No genuine pricing errors found!")
        print("  All price variances are due to informational per-unit pricing displays.")

    print(f"\nOutput saved to: {output_file}")
    print("=" * 80)

    # Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nTotal price records analyzed: {len(df):,}")
    print(f"Prices with informational pricing (excluded): {informational_count:,} ({informational_count/len(df)*100:.1f}%)")
    print(f"Clean prices analyzed: {len(df) - informational_count:,}")
    print(f"Genuine pricing errors found: {len(output_df):,}")

    if len(output_df) > 0:
        error_rate = len(output_df) / (len(df) - informational_count) * 100
        print(f"Error rate: {error_rate:.2f}%")

    print("=" * 80)

if __name__ == "__main__":
    main()
