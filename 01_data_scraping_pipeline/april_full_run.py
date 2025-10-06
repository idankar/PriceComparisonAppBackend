#!/usr/bin/env python3
"""
April.co.il Full Production Run
No page limits - scrapes all products
"""

from april_scraper_db import AprilScraperDB
import logging

logger = logging.getLogger(__name__)

def main():
    """Execute full production scrape"""
    logger.info("\n" + "="*70)
    logger.info("APRIL.CO.IL - FULL PRODUCTION RUN")
    logger.info("="*70 + "\n")

    scraper = AprilScraperDB()

    # Scrape women-perfume category with NO page limit
    total_saved = scraper.run(
        category_url='women-perfume',
        category_name='Women Perfume',
        max_pages=None  # No limit - scrape all pages
    )

    logger.info(f"\n{'='*70}")
    logger.info(f"FULL RUN COMPLETE!")
    logger.info(f"{'='*70}")
    logger.info(f"Total products saved to database: {total_saved}")
    logger.info(f"{'='*70}\n")

if __name__ == "__main__":
    main()
