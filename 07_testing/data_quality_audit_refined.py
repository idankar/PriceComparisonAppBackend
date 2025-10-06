"""
Data Quality Audit Script - REFINED VERSION
============================================
This script audits the product database to identify and flag critical pricing errors,
with improved accuracy to reduce false positives.

Key Improvements:
- Rule #1: Statistical Outlier Detection (>3x median) - UNCHANGED
- Rule #2: Unit/Package Mismatch - REFINED to focus on multi-retailer variance
- Rule #3: Unreasonably Low Price - REFINED with category-specific thresholds

Output: data_quality_audit_refined.csv
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

# Unit/Package mismatch - must have significant price variance across retailers
UNIT_PACKAGE_MIN_RETAILERS = 2  # Must be sold at 2+ retailers
UNIT_PACKAGE_RATIO_THRESHOLD = 15.0  # 15x price difference indicates unit vs package

# Unit price keywords (for supporting evidence only)
UNIT_PRICE_KEYWORDS = [
    'ליחידה', 'per unit', 'לקפסולה', 'per capsule', "ליח'", 'ליח"', 'ליח`'
]

# Category-specific low price thresholds
# Categories where extremely low prices (<₪2) are suspicious
HIGH_VALUE_CATEGORIES = [
    'בשמים', 'בשמי נישה ובוטיק', 'איפור וטיפוח פנים',
    'טבע וויטמינים', 'בית מרקחת', 'Niche Perfume',
    'Women Perfume', 'Face Skin Care', 'Body Care',
    'Vitamins & Supplements', 'Wipes', 'Diapers'
]
HIGH_VALUE_THRESHOLD = 2.00  # ₪2.00

# Excluded categories from low price detection (legitimate cheap items)
EXCLUDED_CATEGORIES = [
    'מוצרים למטבח/חטיפים', 'מוצרי בריאות/חטיפים', 'חטיפים',
    'מזון ומשקאות/חטיפים וממתקים', 'חד פעמי',
    'לבית/ניקיון ותחזוקת הבית/טישיו'  # Tissues can be cheap
]

def get_latest_prices_with_metadata() -> pd.DataFrame:
    """
    Fetch the latest price for each retailer_product_id along with product metadata.
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

def calculate_price_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate price statistics for each barcode across all retailers.
    """
    stats = df.groupby('barcode').agg({
        'price': ['median', 'min', 'max', 'count']
    }).reset_index()

    stats.columns = ['barcode', 'median_price', 'min_price', 'max_price', 'retailer_count']
    stats['price_ratio'] = stats['max_price'] / stats['min_price'].replace(0, np.nan)

    return stats

def detect_outliers(df: pd.DataFrame, price_stats: pd.DataFrame) -> List[Dict]:
    """
    Rule #1: Statistical Outlier Detection
    Flag prices that are more than 3x the median price for the barcode.
    """
    flagged = []
    df_with_stats = df.merge(price_stats, on='barcode', how='left')

    for _, row in df_with_stats.iterrows():
        if row['price'] > (row['median_price'] * OUTLIER_MULTIPLIER):
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'price_ratio': float(row['price'] / row['median_price']),
                'retailer_count': int(row['retailer_count']),
                'reason_for_flag': 'Potential Pricing Error (Outlier)'
            })

    return flagged

def detect_unit_package_mismatch(df: pd.DataFrame, price_stats: pd.DataFrame) -> List[Dict]:
    """
    Rule #2: Unit/Package Mismatch Detection (REFINED)
    Flag products with extreme price variance across multiple retailers,
    indicating one retailer is selling units while others sell packages.

    Criteria:
    - Must be sold at 2+ retailers
    - Price ratio (max/min) > 15x
    """
    flagged = []
    df_with_stats = df.merge(price_stats, on='barcode', how='left')

    # Group by barcode to process each product
    for barcode, group in df_with_stats.groupby('barcode'):
        retailer_count = group['retailer_count'].iloc[0]
        price_ratio = group['price_ratio'].iloc[0]

        # Check if this barcode meets the criteria
        if retailer_count >= UNIT_PACKAGE_MIN_RETAILERS and price_ratio >= UNIT_PACKAGE_RATIO_THRESHOLD:
            # Flag all prices for this barcode
            for _, row in group.iterrows():
                # Check if this specific price is the suspiciously low one
                if row['price'] == group['min_price'].iloc[0]:
                    reason = f'Unit/Package Mismatch (Lowest price for barcode with {price_ratio:.1f}x variance)'
                else:
                    reason = f'Unit/Package Mismatch (Same barcode has {price_ratio:.1f}x price variance)'

                flagged.append({
                    'barcode': row['barcode'],
                    'product_name': row['product_name'],
                    'retailer_name': row['retailer_name'],
                    'suspicious_price': float(row['price']),
                    'median_price_for_barcode': float(row['median_price']),
                    'price_ratio': float(price_ratio),
                    'retailer_count': int(retailer_count),
                    'reason_for_flag': reason
                })

    return flagged

def detect_unreasonably_low_prices(df: pd.DataFrame, price_stats: pd.DataFrame) -> List[Dict]:
    """
    Rule #3: Unreasonably Low Price Detection (REFINED)
    Flag prices that are suspiciously low based on category-specific thresholds.

    - High-value categories (vitamins, cosmetics, etc.): < ₪2.00
    - Excluded categories (snacks, tissues, etc.): No flagging
    """
    flagged = []
    df_with_stats = df.merge(price_stats, on='barcode', how='left')

    for _, row in df_with_stats.iterrows():
        category = str(row['category']) if pd.notna(row['category']) else ''

        # Skip excluded categories (legitimate cheap items)
        if any(excluded in category for excluded in EXCLUDED_CATEGORIES):
            continue

        # Check high-value categories
        is_high_value = any(hv_cat in category for hv_cat in HIGH_VALUE_CATEGORIES)

        if is_high_value and row['price'] < HIGH_VALUE_THRESHOLD:
            flagged.append({
                'barcode': row['barcode'],
                'product_name': row['product_name'],
                'retailer_name': row['retailer_name'],
                'suspicious_price': float(row['price']),
                'median_price_for_barcode': float(row['median_price']),
                'price_ratio': float(row['price'] / row['median_price']) if row['median_price'] > 0 else 1.0,
                'retailer_count': int(row['retailer_count']),
                'reason_for_flag': f'Unreasonably Low Price for Category ({category})'
            })

    return flagged

def main():
    """
    Main execution function.
    """
    print("=" * 80)
    print("DATA QUALITY AUDIT - REFINED PRICING ERROR DETECTION")
    print("=" * 80)

    # Step 1: Fetch data
    print("\n[1/5] Fetching latest prices with metadata from database...")
    df = get_latest_prices_with_metadata()
    print(f"      ✓ Loaded {len(df):,} price records")
    print(f"      ✓ Covering {df['barcode'].nunique():,} unique products")
    print(f"      ✓ From {df['retailer_name'].nunique()} retailers")

    # Step 2: Calculate price statistics
    print("\n[2/5] Calculating price statistics for each barcode...")
    price_stats = calculate_price_stats(df)
    print(f"      ✓ Calculated stats for {len(price_stats):,} barcodes")

    # Show some interesting stats
    multi_retailer = price_stats[price_stats['retailer_count'] >= 2]
    high_variance = price_stats[price_stats['price_ratio'] > 15]
    print(f"      ✓ {len(multi_retailer):,} products sold at 2+ retailers")
    print(f"      ✓ {len(high_variance):,} products with >15x price variance")

    # Step 3: Rule #1 - Statistical Outliers
    print("\n[3/5] Applying Rule #1: Statistical Outlier Detection (>3x median)...")
    outliers = detect_outliers(df, price_stats)
    print(f"      ✓ Flagged {len(outliers):,} outlier prices")

    # Step 4: Rule #2 - Unit/Package Mismatch
    print("\n[4/5] Applying Rule #2: Unit/Package Mismatch Detection (>15x variance)...")
    unit_package_flags = detect_unit_package_mismatch(df, price_stats)
    print(f"      ✓ Flagged {len(unit_package_flags):,} potential unit/package mismatches")

    # Step 5: Rule #3 - Unreasonably Low Prices
    print("\n[5/5] Applying Rule #3: Category-Specific Low Price Detection...")
    low_price_flags = detect_unreasonably_low_prices(df, price_stats)
    print(f"      ✓ Flagged {len(low_price_flags):,} unreasonably low prices")

    # Combine all flags
    all_flags = outliers + unit_package_flags + low_price_flags

    # Create output DataFrame
    output_df = pd.DataFrame(all_flags)

    if len(output_df) > 0:
        # Remove duplicates (a price might be flagged by multiple rules)
        # Keep the first occurrence
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
    output_file = 'data_quality_audit_refined.csv'
    output_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
    print(f"\nTotal Flagged Prices: {len(output_df):,}")

    if len(output_df) > 0:
        print(f"\nTop Issues by Price Ratio:")
        top_10 = output_df.head(10)
        for idx, row in top_10.iterrows():
            print(f"  • {row['product_name'][:50]}: ₪{row['suspicious_price']:.2f} (ratio: {row['price_ratio']:.1f}x)")

        print(f"\nBreakdown by Flag Reason:")
        for reason, count in output_df['reason_for_flag'].value_counts().items():
            print(f"  • {reason}: {count:,}")

    print(f"\nOutput saved to: {output_file}")
    print("=" * 80)

    # Additional insights
    if len(output_df) > 0:
        print("\n" + "=" * 80)
        print("INSIGHTS")
        print("=" * 80)

        # Show most problematic barcodes
        problematic_barcodes = output_df.groupby('barcode').size().sort_values(ascending=False).head(5)
        if len(problematic_barcodes) > 0:
            print("\nMost Problematic Products (by number of flagged prices):")
            for barcode, count in problematic_barcodes.items():
                product_name = output_df[output_df['barcode'] == barcode]['product_name'].iloc[0]
                print(f"  • {product_name[:60]}: {count} prices flagged")

        # Show retailers with most issues
        retailer_issues = output_df['retailer_name'].value_counts().head(5)
        print("\nRetailers with Most Flagged Prices:")
        for retailer, count in retailer_issues.items():
            print(f"  • {retailer}: {count:,} flagged prices")

        print("=" * 80)

if __name__ == "__main__":
    main()
