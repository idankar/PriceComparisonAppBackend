#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import psycopg2
from datetime import datetime
import time

# Database Connection Details
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DATABASE = os.environ.get("PG_DATABASE", "price_comparison_app_v2")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "***REMOVED***")

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )

def count_existing_products(retailer_name):
    """Count existing products for a retailer in the database."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(DISTINCT cp.barcode)
        FROM canonical_products cp
        JOIN retailer_products rp ON cp.barcode = rp.barcode
        JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        JOIN stores s ON p.store_id = s.storeid
        JOIN retailers r ON s.retailerid = r.retailerid
        WHERE LOWER(r.retailername) LIKE %s
          AND p.price > 0
    """, (f'%{retailer_name.lower()}%',))

    count = cur.fetchone()[0]
    conn.close()
    return count

def analyze_scraper_output(output_file, retailer_name):
    """Analyze the output from a scraper."""
    if not os.path.exists(output_file):
        print(f"  ❌ No output file found: {output_file}")
        return

    with open(output_file, 'r') as f:
        lines = f.readlines()

    # Count different types of messages
    stats = {
        'total_lines': len(lines),
        'products_found': 0,
        'barcodes_found': 0,
        'errors': 0,
        'warnings': 0,
        'unique_barcodes': set(),
        'sample_products': []
    }

    for line in lines:
        if 'barcode' in line.lower():
            stats['barcodes_found'] += 1
            # Try to extract barcode
            if '"barcode":' in line or "'barcode':" in line:
                try:
                    # Simple extraction attempt
                    parts = line.split('barcode')[1].split(',')[0]
                    barcode = parts.split(':')[1].strip().strip('"').strip("'")
                    if barcode and barcode.isdigit():
                        stats['unique_barcodes'].add(barcode)
                except:
                    pass

        if 'product' in line.lower() and ('found' in line.lower() or 'processed' in line.lower()):
            stats['products_found'] += 1
            if len(stats['sample_products']) < 5:
                stats['sample_products'].append(line.strip()[:100])

        if 'error' in line.lower():
            stats['errors'] += 1

        if 'warning' in line.lower():
            stats['warnings'] += 1

    print(f"\n  📊 Analysis for {retailer_name}:")
    print(f"     • Total log lines: {stats['total_lines']}")
    print(f"     • Products mentioned: {stats['products_found']}")
    print(f"     • Unique barcodes found: {len(stats['unique_barcodes'])}")
    print(f"     • Errors: {stats['errors']}")
    print(f"     • Warnings: {stats['warnings']}")

    if stats['sample_products']:
        print(f"\n     Sample product lines:")
        for i, sample in enumerate(stats['sample_products'], 1):
            print(f"       {i}. {sample}")

    if stats['unique_barcodes']:
        print(f"\n     Sample barcodes: {list(stats['unique_barcodes'])[:10]}")

    return stats

def run_scraper_with_monitoring(script_path, retailer_name, days=1):
    """Run a scraper and monitor its output."""
    print(f"\n{'='*60}")
    print(f"Running {retailer_name} Scraper")
    print(f"{'='*60}")

    # Check products before
    before_count = count_existing_products(retailer_name)
    print(f"Products in DB before: {before_count:,}")

    # Prepare output file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"{retailer_name.lower().replace(' ', '_')}_{timestamp}.log"

    # Run the scraper
    print(f"\n🔄 Starting scraper: {script_path}")
    print(f"   Output will be saved to: {output_file}")

    cmd = ['python3', script_path]
    if 'barcode_matching' in script_path:
        cmd.extend(['--days', str(days)])

    start_time = time.time()

    try:
        with open(output_file, 'w') as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # Monitor the output file size
            print("\n⏳ Scraper is running...")
            last_size = 0
            no_change_count = 0

            while process.poll() is None:
                time.sleep(5)
                if os.path.exists(output_file):
                    current_size = os.path.getsize(output_file)
                    if current_size != last_size:
                        print(f"   Log size: {current_size:,} bytes (growing...)")
                        last_size = current_size
                        no_change_count = 0
                    else:
                        no_change_count += 1
                        if no_change_count > 12:  # No change for 1 minute
                            print("   ⚠️ No new output for 60 seconds")

            return_code = process.wait()
            elapsed = time.time() - start_time

            if return_code == 0:
                print(f"\n✅ Scraper completed successfully in {elapsed:.1f} seconds")
            else:
                print(f"\n⚠️ Scraper exited with code {return_code} after {elapsed:.1f} seconds")

    except Exception as e:
        print(f"\n❌ Error running scraper: {e}")
        return None

    # Analyze the output
    stats = analyze_scraper_output(output_file, retailer_name)

    # Wait a bit for database to update
    print("\n⏳ Waiting 10 seconds for database updates...")
    time.sleep(10)

    # Check products after
    after_count = count_existing_products(retailer_name)
    print(f"\nProducts in DB after: {after_count:,}")
    print(f"Change: {after_count - before_count:+,} products")

    return {
        'before': before_count,
        'after': after_count,
        'change': after_count - before_count,
        'log_file': output_file,
        'stats': stats
    }

def main():
    """Main function to run and monitor pharmacy scrapers."""
    print("="*70)
    print("PHARMACY SCRAPER MONITORING AND ANALYSIS")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    results = {}

    # Run Super-Pharm scrapers
    print("\n" + "="*70)
    print("SUPER-PHARM SCRAPERS")
    print("="*70)

    # Commercial scraper
    if os.path.exists('01_data_scraping_pipeline/Super_pharm_scrapers/super_pharm_scraper.py'):
        print("\n1. Super-Pharm Commercial Scraper")
        result = run_scraper_with_monitoring(
            '01_data_scraping_pipeline/Super_pharm_scrapers/super_pharm_scraper.py',
            'Super-Pharm Commercial',
            days=1
        )
        results['super_pharm_commercial'] = result

    # Portal scraper (barcode matching)
    if os.path.exists('01_data_scraping_pipeline/Super_pharm_scrapers/super_pharm_barcode_matching.py'):
        print("\n2. Super-Pharm Portal Scraper (Barcode Matching)")
        result = run_scraper_with_monitoring(
            '01_data_scraping_pipeline/Super_pharm_scrapers/super_pharm_barcode_matching.py',
            'Super-Pharm Portal',
            days=1
        )
        results['super_pharm_portal'] = result

    # Run Good Pharm scrapers
    print("\n" + "="*70)
    print("GOOD PHARM SCRAPERS")
    print("="*70)

    # Commercial scraper
    if os.path.exists('01_data_scraping_pipeline/Good_pharm_scrapers/good_pharm_scraper.py'):
        print("\n3. Good Pharm Commercial Scraper")
        result = run_scraper_with_monitoring(
            '01_data_scraping_pipeline/Good_pharm_scrapers/good_pharm_scraper.py',
            'Good Pharm Commercial',
            days=1
        )
        results['good_pharm_commercial'] = result

    # Portal scraper (barcode matching)
    if os.path.exists('01_data_scraping_pipeline/Good_pharm_scrapers/good_pharm_barcode_matching_fixed.py'):
        print("\n4. Good Pharm Portal Scraper (Barcode Matching)")
        result = run_scraper_with_monitoring(
            '01_data_scraping_pipeline/Good_pharm_scrapers/good_pharm_barcode_matching_fixed.py',
            'Good Pharm Portal',
            days=1
        )
        results['good_pharm_portal'] = result

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    total_new = 0
    for name, result in results.items():
        if result:
            print(f"\n{name}:")
            print(f"  • Products before: {result['before']:,}")
            print(f"  • Products after: {result['after']:,}")
            print(f"  • Change: {result['change']:+,}")
            print(f"  • Log file: {result['log_file']}")
            total_new += result['change']

    print(f"\n📊 Total new products added: {total_new:+,}")

    # Re-run overlap analysis
    print("\n" + "="*70)
    print("RE-RUNNING OVERLAP ANALYSIS")
    print("="*70)

    if os.path.exists('analyze_pharmacy_overlap.py'):
        subprocess.run(['python3', 'analyze_pharmacy_overlap.py'])

if __name__ == "__main__":
    main()