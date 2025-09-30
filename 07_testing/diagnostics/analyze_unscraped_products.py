#!/usr/bin/env python3
"""
Analyze Unscraped Products to Understand What's Missing

This script samples products that exist in government data but weren't scraped
from commercial websites, and categorizes them to understand if they're pharmacy-related.
"""

import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import re
from collections import Counter, defaultdict


def get_database_connection():
    """Establish connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="price_comparison_app_v2",
            user="postgres",
            password="***REMOVED***",
            cursor_factory=DictCursor
        )
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return None


def categorize_product(name, brand=None):
    """
    Categorize a product based on its name and brand.
    Returns category and confidence level.
    """
    name_lower = (name or '').lower()
    brand_lower = (brand or '').lower()

    # Define category patterns
    categories = {
        'cosmetics': {
            'keywords': ['איפור', 'מייק אפ', 'שפתון', 'ליפסטיק', 'מסקרה', 'אודם',
                        'פודרה', 'סומק', 'קונסילר', 'פריימר', 'ברונזר', 'היילייטר',
                        'צלליות', 'עפרון', 'גלוס', 'לק', 'מניקור', 'פדיקור'],
            'brands': ['מייבלין', 'לוריאל', 'רבלון', 'מקס פקטור', 'בורז׳ואה']
        },
        'skincare': {
            'keywords': ['קרם', 'סרום', 'לחות', 'ניקוי', 'פילינג', 'מסכה', 'טונר',
                        'עיניים', 'פנים', 'גוף', 'ידיים', 'רגליים', 'שמש', 'spf',
                        'אנטי אייג׳ינג', 'קמטים', 'דרמו', 'אקנה', 'פצעים'],
            'brands': ['וישי', 'לה רוש', 'אוון', 'יוריאז׳', 'ביודרמה', 'סבמד', 'ניוטרוג׳ינה']
        },
        'hair_care': {
            'keywords': ['שמפו', 'מרכך', 'מסכה לשיער', 'ג׳ל', 'ווקס', 'ספריי לשיער',
                        'קרם לשיער', 'שיער', 'קרקפת', 'קשקשים', 'צבע שיער', 'חינה'],
            'brands': ['הד אנד שולדרס', 'פנטן', 'דאב', 'טרזמה', 'לוריאל']
        },
        'oral_care': {
            'keywords': ['משחת שיניים', 'מברשת שיניים', 'שטיפת פה', 'ליסטרין',
                        'חוט דנטלי', 'שיניים', 'חניכיים', 'הלבנה', 'רגישות'],
            'brands': ['אורל בי', 'קולגייט', 'סנסודיין', 'פרודונטקס']
        },
        'deodorant': {
            'keywords': ['דאודורנט', 'אנטיפרספירנט', 'רול און', 'ספריי', 'סטיק'],
            'brands': ['רקסונה', 'דאב', 'אקס', 'נויבה', 'ספיד סטיק']
        },
        'baby_care': {
            'keywords': ['תינוק', 'בייבי', 'חיתול', 'פמפרס', 'האגיס', 'מגבון',
                        'אבקת תינוקות', 'שמן תינוקות', 'מוצץ', 'בקבוק'],
            'brands': ['פמפרס', 'האגיס', 'בייבי ליין', 'ג׳ונסון']
        },
        'feminine_care': {
            'keywords': ['תחבושת', 'טמפון', 'פד', 'היגיינה נשית', 'אינטימי'],
            'brands': ['אולוויז', 'קוטקס', 'קרפרי']
        },
        'vitamins': {
            'keywords': ['ויטמין', 'תוסף', 'כמוסה', 'טבליה', 'סירופ', 'אומגה',
                        'פרוביוטיקה', 'מינרל', 'ברזל', 'סידן', 'מגנזיום', 'd3', 'b12'],
            'brands': ['סולגר', 'אלטמן', 'סופהרב', 'נוטרילייט', 'ביו גארד']
        },
        'medical_devices': {
            'keywords': ['מד חום', 'מד לחץ', 'גלוקומטר', 'משאף', 'נבולייזר',
                        'תחבושת', 'פלסטר', 'גזה', 'אלכוהול', 'בטדין'],
            'brands': ['בראון', 'אומרון', 'בייר']
        },
        'food_supplements': {
            'keywords': ['אנשור', 'סימילאק', 'תחליף חלב', 'דייסה', 'מטרנה'],
            'brands': ['אנשור', 'סימילאק', 'נוטרילון', 'מטרנה']
        },
        'household': {
            'keywords': ['סבון כביסה', 'מרכך כביסה', 'אבקת כביסה', 'מטלית',
                        'נייר טואלט', 'מפית', 'כלים חד פעמיים', 'שקית'],
            'brands': ['סנו', 'ביומט', 'אריאל', 'פרסיל']
        },
        'food': {
            'keywords': ['שוקולד', 'ממתק', 'סוכריה', 'גומי', 'חטיף', 'במבה',
                        'ביסלי', 'שתייה', 'מיץ', 'קפה', 'תה'],
            'brands': ['עלית', 'שטראוס', 'אסם', 'קוקה קולה']
        }
    }

    # Check each category
    for category, patterns in categories.items():
        # Check keywords
        for keyword in patterns['keywords']:
            if keyword in name_lower:
                return category, 'high'

        # Check brands
        for brand_pattern in patterns['brands']:
            if brand_pattern in name_lower or brand_pattern in brand_lower:
                return category, 'high'

    # Try to identify pharmacy-related terms
    pharmacy_indicators = ['רוקח', 'מרשם', 'תרופה', 'קפסולה', 'סירופ', 'משחה', 'טיפות', 'ג׳ל']
    for indicator in pharmacy_indicators:
        if indicator in name_lower:
            return 'pharmacy_other', 'medium'

    # Check if it's clearly non-pharmacy
    non_pharmacy = ['צעצוע', 'משחק', 'כרטיס', 'מתנה', 'ספר', 'מחברת', 'עט', 'בגד', 'נעל']
    for term in non_pharmacy:
        if term in name_lower:
            return 'non_pharmacy', 'high'

    return 'uncategorized', 'low'


def analyze_unscraped_products_by_retailer(cursor):
    """
    Analyze unscraped products grouped by retailer.
    """
    print("\n" + "="*80)
    print("ANALYSIS OF UNSCRAPED PRODUCTS BY RETAILER")
    print("="*80)

    # Get breakdown by retailer
    query = """
        WITH unscraped AS (
            SELECT
                rp.barcode,
                rp.original_retailer_name as name,
                r.retailername,
                cp.brand,
                COUNT(DISTINCT p.price_id) as price_count
            FROM retailer_products rp
            JOIN retailers r ON rp.retailer_id = r.retailerid
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
            WHERE rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND NOT EXISTS (
                SELECT 1
                FROM canonical_products cp2
                WHERE cp2.barcode = rp.barcode
                AND cp2.source_retailer_id IS NOT NULL
            )
            GROUP BY rp.barcode, rp.original_retailer_name, r.retailername, cp.brand
        )
        SELECT
            retailername,
            COUNT(DISTINCT barcode) as unscraped_count,
            SUM(price_count) as total_price_points
        FROM unscraped
        GROUP BY retailername
        ORDER BY unscraped_count DESC;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("\nUnscraped Products by Retailer:")
    print("-" * 60)
    for row in results:
        print(f"{row['retailername']:20} {row['unscraped_count']:8,} products ({row['total_price_points']:,} price points)")

    return results


def sample_unscraped_products(cursor, limit=200):
    """
    Get a sample of unscraped products for analysis.
    """
    print("\n" + "="*80)
    print("SAMPLING UNSCRAPED PRODUCTS")
    print("="*80)

    # Get sample of unscraped products with high price activity
    query = """
        SELECT DISTINCT
            rp.barcode,
            rp.original_retailer_name as name,
            r.retailername,
            cp.brand,
            COUNT(DISTINCT p.price_id) as price_count
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        LEFT JOIN canonical_products cp ON rp.barcode = cp.barcode
        WHERE rp.barcode IS NOT NULL
        AND rp.barcode != ''
        AND NOT EXISTS (
            SELECT 1
            FROM canonical_products cp2
            WHERE cp2.barcode = rp.barcode
            AND cp2.source_retailer_id IS NOT NULL
        )
        GROUP BY rp.barcode, rp.original_retailer_name, r.retailername, cp.brand
        ORDER BY price_count DESC
        LIMIT %s;
    """

    cursor.execute(query, (limit,))
    products = cursor.fetchall()

    # Categorize products
    categories_count = Counter()
    category_samples = defaultdict(list)
    retailer_categories = defaultdict(lambda: defaultdict(int))

    for product in products:
        category, confidence = categorize_product(product['name'], product['brand'])
        categories_count[category] += 1
        retailer_categories[product['retailername']][category] += 1

        # Store samples for each category
        if len(category_samples[category]) < 5:
            category_samples[category].append({
                'barcode': product['barcode'],
                'name': product['name'],
                'brand': product['brand'],
                'retailer': product['retailername'],
                'price_count': product['price_count']
            })

    return categories_count, category_samples, retailer_categories


def print_category_analysis(categories_count, category_samples, retailer_categories):
    """
    Print detailed category analysis.
    """
    print("\n" + "="*80)
    print("PRODUCT CATEGORY BREAKDOWN")
    print("="*80)

    # Calculate totals
    total = sum(categories_count.values())
    pharmacy_related = ['cosmetics', 'skincare', 'hair_care', 'oral_care', 'deodorant',
                       'baby_care', 'feminine_care', 'vitamins', 'medical_devices',
                       'food_supplements', 'pharmacy_other']

    pharmacy_count = sum(categories_count[cat] for cat in pharmacy_related if cat in categories_count)
    non_pharmacy_count = total - pharmacy_count

    print(f"\nTotal sampled: {total}")
    print(f"Pharmacy-related: {pharmacy_count} ({pharmacy_count*100/total:.1f}%)")
    print(f"Non-pharmacy: {non_pharmacy_count} ({non_pharmacy_count*100/total:.1f}%)")

    # Sort categories by count
    sorted_categories = sorted(categories_count.items(), key=lambda x: x[1], reverse=True)

    print("\n" + "-"*60)
    print("Category Distribution:")
    print("-"*60)
    for category, count in sorted_categories:
        pct = count * 100 / total
        category_type = "💊" if category in pharmacy_related else "📦"
        print(f"{category_type} {category:20} {count:4} ({pct:5.1f}%)")

    print("\n" + "="*80)
    print("SAMPLE PRODUCTS BY CATEGORY")
    print("="*80)

    # Show samples for each category
    for category in sorted_categories[:10]:  # Top 10 categories
        cat_name = category[0]
        print(f"\n{cat_name.upper().replace('_', ' ')} (Count: {categories_count[cat_name]}):")
        print("-" * 60)

        samples = category_samples[cat_name]
        for i, sample in enumerate(samples[:3], 1):  # Show up to 3 samples
            print(f"  {i}. [{sample['barcode']}] {sample['name'][:60]}")
            if sample['brand']:
                print(f"     Brand: {sample['brand']}")
            print(f"     Retailer: {sample['retailer']}, Price points: {sample['price_count']}")

    print("\n" + "="*80)
    print("RETAILER-SPECIFIC BREAKDOWN")
    print("="*80)

    for retailer, categories in retailer_categories.items():
        print(f"\n{retailer}:")
        total_retailer = sum(categories.values())
        for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]:
            pct = count * 100 / total_retailer
            print(f"  {category:20} {count:3} ({pct:5.1f}%)")


def analyze_specific_patterns(cursor):
    """
    Look for specific patterns in unscraped products.
    """
    print("\n" + "="*80)
    print("SPECIFIC PATTERN ANALYSIS")
    print("="*80)

    # Check for products with Hebrew brand names (might be local/generic)
    query_hebrew_brands = """
        SELECT DISTINCT
            cp.brand,
            COUNT(DISTINCT cp.barcode) as product_count
        FROM canonical_products cp
        WHERE cp.source_retailer_id IS NULL
        AND cp.brand IS NOT NULL
        AND cp.brand != ''
        AND cp.brand ~ '[א-ת]'  -- Contains Hebrew characters
        GROUP BY cp.brand
        ORDER BY product_count DESC
        LIMIT 20;
    """

    cursor.execute(query_hebrew_brands)
    hebrew_brands = cursor.fetchall()

    if hebrew_brands:
        print("\nTop Hebrew/Local Brands Not Scraped:")
        print("-" * 40)
        for brand in hebrew_brands:
            print(f"  {brand['brand']:30} {brand['product_count']:4} products")

    # Check for products with specific keywords
    keyword_queries = [
        ("Generic/Store Brand", "לייף|life|סופר פארם|BE|ביי|גוד פארם"),
        ("International Brands", "johnson|pampers|dove|nivea|loreal|maybelline"),
        ("Food Items", "במבה|ביסלי|שוקולד|ממתק|חטיף|קפה|תה"),
        ("Non-Pharmacy Items", "צעצוע|משחק|מתנה|כרטיס|נייר|מטלית")
    ]

    print("\n" + "-"*60)
    print("Keyword Pattern Analysis:")
    print("-"*60)

    for category, pattern in keyword_queries:
        query = """
            SELECT COUNT(DISTINCT rp.barcode) as count
            FROM retailer_products rp
            WHERE rp.barcode IS NOT NULL
            AND rp.barcode != ''
            AND rp.original_retailer_name ~* %s
            AND NOT EXISTS (
                SELECT 1
                FROM canonical_products cp
                WHERE cp.barcode = rp.barcode
                AND cp.source_retailer_id IS NOT NULL
            );
        """
        cursor.execute(query, (pattern,))
        result = cursor.fetchone()
        print(f"  {category:25} {result['count']:6,} products")


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("UNSCRAPED PRODUCTS ANALYSIS")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to database
    conn = get_database_connection()
    if not conn:
        return

    cursor = conn.cursor()

    try:
        # Analyze by retailer
        analyze_unscraped_products_by_retailer(cursor)

        # Sample and categorize products
        categories_count, category_samples, retailer_categories = sample_unscraped_products(cursor, limit=500)

        # Print analysis
        print_category_analysis(categories_count, category_samples, retailer_categories)

        # Analyze specific patterns
        analyze_specific_patterns(cursor)

        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)

    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()