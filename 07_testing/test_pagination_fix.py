#!/usr/bin/env python3
"""
Test pagination on a known multi-page category
"""
import sys
sys.path.insert(0, '/Users/idankarbat/Documents/noa_recovery/Desktop/PriceComparisonApp/01_data_scraping_pipeline/Super_pharm_scrapers')

from super_pharm_scraper import SuperPharmScraper

# Override categories with just "Hair Care" which we know has multiple pages
scraper = SuperPharmScraper(dry_run=False, headless=True, test_mode=False)
scraper.categories = [{"name": "טיפוח השיער", "url": "https://shop.super-pharm.co.il/care/hair-care/c/15170000"}]
scraper.test_mode_limit = 999  # Allow all pages

print("🧪 Testing pagination on Hair Care category...")
scraper.scrape()
