#!/usr/bin/env python3
"""
April.co.il - Complete Multi-Category Scraper
Scrapes all product categories across the entire site
"""

from april_scraper_db import AprilScraperDB
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Comprehensive category list for april.co.il
# Organized by main categories and their subcategories
CATEGORIES = [
    # Perfume categories
    {'url': 'women-perfume', 'name': 'Women Perfume'},
    {'url': 'men-perfume', 'name': 'Men Perfume'},
    {'url': 'niche-perfume', 'name': 'Niche Perfume'},
    {'url': 'home-scents-diffusers', 'name': 'Home Scents & Diffusers'},

    # Makeup categories
    {'url': 'face-makeup', 'name': 'Face Makeup'},
    {'url': 'eye-makeup', 'name': 'Eye Makeup'},
    {'url': 'lips', 'name': 'Lip Makeup'},
    {'url': 'makeup-brush-and-accessories', 'name': 'Makeup Brushes & Accessories'},

    # Skincare categories
    {'url': 'face-skin-care', 'name': 'Face Skin Care'},
    {'url': 'face-cleaning', 'name': 'Face Cleaning'},
    {'url': 'body-care', 'name': 'Body Care'},
    {'url': 'sun-block', 'name': 'Sun Protection'},
    {'url': 'men-skin-care', 'name': 'Men Skin Care'},

    # Hair care categories
    {'url': 'shampoo-and-conditioner', 'name': 'Shampoo & Conditioner'},
    {'url': 'nurture-and-repair', 'name': 'Hair Treatment & Styling'},
    {'url': 'hair-accessories', 'name': 'Hair Accessories'},

    # Special categories
    {'url': 'online-outlet', 'name': 'Online Outlet'},
    {'url': 'sale-items', 'name': 'Sale Items'},
    {'url': 'travel-size', 'name': 'Travel Size'},

    # Gift sets
    {'url': 'perfume-sets-and-gifts', 'name': 'Perfume Sets & Gifts'},
    {'url': 'beauty-sets-and-gifts', 'name': 'Beauty Sets & Gifts'},
    {'url': 'hair-sets-and-gifts', 'name': 'Hair Sets & Gifts'},
]


def main():
    """Execute complete multi-category scrape"""

    start_time = datetime.now()

    logger.info("\n" + "="*80)
    logger.info("APRIL.CO.IL - COMPLETE MULTI-CATEGORY PRODUCTION RUN")
    logger.info("="*80)
    logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total categories to scrape: {len(CATEGORIES)}")
    logger.info("="*80 + "\n")

    # Track overall statistics
    overall_stats = {
        'total_categories_attempted': 0,
        'total_categories_successful': 0,
        'total_categories_failed': 0,
        'total_products_saved': 0,
        'total_pages_scraped': 0,
        'category_results': []
    }

    # Scrape each category
    for idx, category in enumerate(CATEGORIES, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"CATEGORY {idx}/{len(CATEGORIES)}: {category['name']}")
        logger.info(f"{'='*80}\n")

        overall_stats['total_categories_attempted'] += 1

        try:
            # Create new scraper instance for each category
            scraper = AprilScraperDB()

            # Scrape category (no page limit)
            products_saved = scraper.run(
                category_url=category['url'],
                category_name=category['name'],
                max_pages=None  # Scrape all pages
            )

            # Record results
            category_result = {
                'name': category['name'],
                'url': category['url'],
                'products_saved': products_saved,
                'pages_scraped': scraper.stats['pages_scraped'],
                'status': 'success' if products_saved > 0 else 'empty'
            }

            overall_stats['category_results'].append(category_result)
            overall_stats['total_products_saved'] += products_saved
            overall_stats['total_pages_scraped'] += scraper.stats['pages_scraped']

            if products_saved > 0:
                overall_stats['total_categories_successful'] += 1
                logger.info(f"✓ Category complete: {products_saved} products saved")
            else:
                logger.info(f"⚠ Category empty or unavailable")

        except Exception as e:
            logger.error(f"✗ Category failed: {e}")
            overall_stats['total_categories_failed'] += 1
            overall_stats['category_results'].append({
                'name': category['name'],
                'url': category['url'],
                'products_saved': 0,
                'pages_scraped': 0,
                'status': 'failed',
                'error': str(e)
            })

    # Final summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info("\n" + "="*80)
    logger.info("COMPLETE MULTI-CATEGORY RUN - FINAL SUMMARY")
    logger.info("="*80)
    logger.info(f"Start time:              {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"End time:                {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total duration:          {duration:.2f} seconds ({duration/60:.2f} minutes)")
    logger.info("")
    logger.info(f"Categories attempted:    {overall_stats['total_categories_attempted']}")
    logger.info(f"Categories successful:   {overall_stats['total_categories_successful']}")
    logger.info(f"Categories failed:       {overall_stats['total_categories_failed']}")
    logger.info("")
    logger.info(f"Total pages scraped:     {overall_stats['total_pages_scraped']}")
    logger.info(f"Total products saved:    {overall_stats['total_products_saved']}")
    logger.info("")
    logger.info("Category Breakdown:")
    logger.info("-" * 80)

    for result in overall_stats['category_results']:
        status_symbol = "✓" if result['status'] == 'success' else "✗" if result['status'] == 'failed' else "○"
        logger.info(f"{status_symbol} {result['name']:40} | Products: {result['products_saved']:4} | Pages: {result['pages_scraped']:3}")

    logger.info("="*80)

    # Save summary to file
    import json
    with open('april_full_run_summary.json', 'w', encoding='utf-8') as f:
        json.dump({
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'statistics': overall_stats
        }, f, indent=2, ensure_ascii=False)

    logger.info("\n✓ Summary saved to april_full_run_summary.json\n")

    return overall_stats


if __name__ == "__main__":
    stats = main()

    logger.info(f"\n{'='*80}")
    logger.info(f"🎉 SCRAPING COMPLETE!")
    logger.info(f"{'='*80}")
    logger.info(f"Added {stats['total_products_saved']} products to the database!")
    logger.info(f"{'='*80}\n")
