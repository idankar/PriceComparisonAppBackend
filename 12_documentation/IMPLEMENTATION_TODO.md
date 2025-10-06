# IMPLEMENTATION TODO LIST
## Data Gaps Resolution - Action Items

**Status:** Ready to Execute
**Date:** October 2, 2025
**Estimated Total Time:** 5-7 days

---

## 🚀 PHASE 1: GOOD PHARM COVERAGE FIX (DAYS 1-2)

### Task 1.1: Database Backup
**Priority:** CRITICAL
**Time:** 30 minutes
**Owner:** Data Engineer

- [ ] Backup canonical_products table
  ```bash
  pg_dump -h localhost -U postgres -d price_comparison_app_v2 \
    -t canonical_products \
    --data-only --insert \
    > backup_canonical_products_$(date +%Y%m%d).sql
  ```

- [ ] Backup retailer_products table
  ```bash
  pg_dump -h localhost -U postgres -d price_comparison_app_v2 \
    -t retailer_products \
    --data-only --insert \
    > backup_retailer_products_$(date +%Y%m%d).sql
  ```

- [ ] Document current counts
  ```sql
  SELECT source_retailer_id, is_active, COUNT(*)
  FROM canonical_products
  GROUP BY source_retailer_id, is_active;
  ```

### Task 1.2: Create Good Pharm Entries from Orphaned Products
**Priority:** HIGH
**Time:** 1 hour (includes testing)
**Expected Impact:** +3,031 products

- [ ] Test query on small sample (LIMIT 10)
  ```sql
  -- TEST QUERY
  SELECT
    cp.barcode, cp.name, cp.brand, cp.category,
    cp.image_url, 97 as source_retailer_id
  FROM canonical_products cp
  JOIN retailer_products rp ON cp.barcode = rp.barcode
  WHERE cp.source_retailer_id IS NULL
    AND rp.retailer_id = 97
    AND NOT EXISTS (
      SELECT 1 FROM canonical_products cp2
      WHERE cp2.barcode = cp.barcode AND cp2.source_retailer_id = 97
    )
  LIMIT 10;
  ```

- [ ] Validate test results (check 10 products manually)

- [ ] Run full INSERT query
  ```sql
  INSERT INTO canonical_products (
    barcode, name, brand, category, image_url,
    source_retailer_id, is_active, created_at, updated_at
  )
  SELECT
    cp.barcode, cp.name, cp.brand, cp.category, cp.image_url,
    97, TRUE, NOW(), NOW()
  FROM canonical_products cp
  JOIN retailer_products rp ON cp.barcode = rp.barcode
  WHERE cp.source_retailer_id IS NULL
    AND rp.retailer_id = 97
    AND NOT EXISTS (
      SELECT 1 FROM canonical_products cp2
      WHERE cp2.barcode = cp.barcode AND cp2.source_retailer_id = 97
    );
  ```

- [ ] Verify insertion count (should be ≈ 3,031)
  ```sql
  SELECT COUNT(*) FROM canonical_products
  WHERE source_retailer_id = 97 AND is_active = TRUE;
  ```

- [ ] Spot-check 20 random products for correctness

### Task 1.3: Create Good Pharm Entries from Super-Pharm
**Priority:** HIGH
**Time:** 1 hour
**Expected Impact:** +5,584 products (accounting for overlap with orphaned)

- [ ] Test query on small sample (LIMIT 10)

- [ ] Run INSERT query
  ```sql
  INSERT INTO canonical_products (
    barcode, name, brand, category, image_url,
    source_retailer_id, is_active, created_at, updated_at
  )
  SELECT DISTINCT
    sp.barcode, sp.name, sp.brand, sp.category, sp.image_url,
    97, TRUE, NOW(), NOW()
  FROM canonical_products sp
  JOIN retailer_products rp ON sp.barcode = rp.barcode
  WHERE sp.source_retailer_id = 52
    AND rp.retailer_id = 97
    AND sp.image_url IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM canonical_products cp2
      WHERE cp2.barcode = sp.barcode AND cp2.source_retailer_id = 97
    );
  ```

- [ ] Verify count increase

- [ ] Check no duplicates exist
  ```sql
  SELECT barcode, COUNT(*)
  FROM canonical_products
  WHERE source_retailer_id = 97
  GROUP BY barcode
  HAVING COUNT(*) > 1;
  ```

### Task 1.4: Create Good Pharm Entries from Be Pharm
**Priority:** HIGH
**Time:** 1 hour
**Expected Impact:** +3,161 products (accounting for overlaps)

- [ ] Test query on small sample

- [ ] Run INSERT query
  ```sql
  INSERT INTO canonical_products (
    barcode, name, brand, category, image_url,
    source_retailer_id, is_active, created_at, updated_at
  )
  SELECT DISTINCT
    bp.barcode, bp.name, bp.brand, bp.category, bp.image_url,
    97, TRUE, NOW(), NOW()
  FROM canonical_products bp
  JOIN retailer_products rp ON bp.barcode = rp.barcode
  WHERE bp.source_retailer_id = 150
    AND rp.retailer_id = 97
    AND bp.image_url IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM canonical_products cp2
      WHERE cp2.barcode = bp.barcode AND cp2.source_retailer_id = 97
    );
  ```

- [ ] Verify final Good Pharm count (should be ≈ 11,776)

- [ ] Run comprehensive validation
  ```sql
  -- Should return 0
  SELECT COUNT(*) FROM canonical_products
  WHERE source_retailer_id = 97
    AND (image_url IS NULL OR category IS NULL OR name IS NULL);
  ```

### Task 1.5: Validation & Testing
**Priority:** CRITICAL
**Time:** 2 hours

- [ ] Verify total active products ≈ 40,348
  ```sql
  SELECT COUNT(*) FROM canonical_products WHERE is_active = TRUE;
  ```

- [ ] Check Good Pharm products have pricing
  ```sql
  SELECT
    COUNT(DISTINCT cp.barcode) as total_gp_active,
    COUNT(DISTINCT rp.barcode) as gp_with_pricing,
    ROUND(COUNT(DISTINCT rp.barcode)::numeric / COUNT(DISTINCT cp.barcode) * 100, 1) as pct
  FROM canonical_products cp
  LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode AND rp.retailer_id = 97
  WHERE cp.source_retailer_id = 97 AND cp.is_active = TRUE;
  ```

- [ ] Test frontend displays Good Pharm products

- [ ] Manual validation of 50 random Good Pharm products
  ```sql
  SELECT * FROM canonical_products
  WHERE source_retailer_id = 97 AND is_active = TRUE
  ORDER BY RANDOM()
  LIMIT 50;
  ```

- [ ] Check image URLs are accessible (sample 20)

- [ ] Document Phase 1 completion metrics

---

## 🚀 PHASE 2: SUPER-PHARM ONLINE STORE (DAYS 3-5)

### Task 2.1: Create Online Store in Database
**Priority:** HIGH
**Time:** 15 minutes

- [ ] Check if store ID 52001 is available
  ```sql
  SELECT * FROM stores WHERE storeid = 52001;
  ```

- [ ] Insert Super-Pharm Online Store
  ```sql
  INSERT INTO stores (
    storeid, storename, retailerid, chainid,
    storetypecode, isactive, createdat
  )
  VALUES (
    52001,
    'Super-Pharm Online Store',
    52,
    '7290172900007',
    'ONLINE',
    TRUE,
    NOW()
  );
  ```

- [ ] Verify store created
  ```sql
  SELECT * FROM stores WHERE storeid = 52001;
  ```

### Task 2.2: Modify Super-Pharm Scraper
**Priority:** HIGH
**Time:** 6-8 hours
**File:** `01_data_scraping_pipeline/Super_pharm_scrapers/super_pharm_scraper.py`

- [ ] Read current scraper implementation

- [ ] Add online price extraction function
  ```python
  def scrape_online_price(product_url: str) -> Optional[Dict]:
      """Extract price and availability from product page"""
      # Implementation details in architecture doc
  ```

- [ ] Add retailer_products insertion logic
  ```python
  def create_retailer_product(barcode: str) -> int:
      """Create or get retailer_product_id"""
      # Implementation details in architecture doc
  ```

- [ ] Add online price insertion function
  ```python
  def insert_online_price(retailer_product_id: int, price: float):
      """Insert price linked to online store"""
      # Implementation details in architecture doc
  ```

- [ ] Modify main scraping loop to call new functions

- [ ] Add rate limiting (delay between requests)

- [ ] Add error handling and logging

- [ ] Add checkpoint/resume capability for long runs

### Task 2.3: Test Scraper on Sample Products
**Priority:** HIGH
**Time:** 2 hours

- [ ] Create test script with 10 products
  ```python
  TEST_PRODUCTS = [
      "1244444434932",
      "8698532592210",
      # ... 8 more
  ]
  ```

- [ ] Run scraper on test products

- [ ] Verify retailer_products entries created

- [ ] Verify prices inserted correctly

- [ ] Check prices linked to store 52001

- [ ] Validate price values are reasonable

### Task 2.4: Run Full Backfill for 9,590 Products
**Priority:** HIGH
**Time:** 4-8 hours (depending on rate limiting)

- [ ] Get list of products needing prices
  ```sql
  COPY (
    SELECT barcode, name
    FROM canonical_products
    WHERE source_retailer_id = 52
      AND is_active = TRUE
      AND NOT EXISTS (
        SELECT 1 FROM retailer_products rp
        WHERE rp.barcode = canonical_products.barcode
        AND rp.retailer_id = 52
      )
  ) TO '/tmp/sp_products_to_scrape.csv' WITH CSV HEADER;
  ```

- [ ] Create backfill script
  ```python
  # File: 04_utilities/super_pharm_online_backfill.py
  ```

- [ ] Run backfill with checkpointing

- [ ] Monitor progress (log every 100 products)

- [ ] Handle failures gracefully

- [ ] Verify final counts
  ```sql
  SELECT
    COUNT(*) FILTER (WHERE is_active = TRUE) as active,
    COUNT(DISTINCT rp.barcode) as with_pricing
  FROM canonical_products cp
  LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode AND rp.retailer_id = 52
  WHERE cp.source_retailer_id = 52;
  ```

### Task 2.5: Schedule Daily Online Scraper
**Priority:** MEDIUM
**Time:** 1 hour

- [ ] Create cron job or scheduled task
  ```bash
  # Run daily at 2 AM
  0 2 * * * cd /path/to/project && python3 super_pharm_scraper.py --online-only
  ```

- [ ] Add monitoring/alerting for failures

- [ ] Set up log rotation

- [ ] Test scheduled execution

### Task 2.6: Validation & Testing
**Priority:** CRITICAL
**Time:** 2 hours

- [ ] Verify Super-Pharm pricing coverage = 100%
  ```sql
  SELECT
    COUNT(*) as total_active,
    COUNT(DISTINCT rp.barcode) as with_pricing,
    ROUND(COUNT(DISTINCT rp.barcode)::numeric / COUNT(*) * 100, 1) as pct
  FROM canonical_products cp
  LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode AND rp.retailer_id = 52
  WHERE cp.source_retailer_id = 52 AND cp.is_active = TRUE;
  ```

- [ ] Check online store has prices
  ```sql
  SELECT COUNT(*) FROM prices WHERE store_id = 52001;
  ```

- [ ] Test price comparison feature (online vs. stores)

- [ ] Validate 30 random products on website

- [ ] Document Phase 2 completion metrics

---

## 🚀 PHASE 3: DATA QUALITY IMPROVEMENTS (DAYS 6-7)

### Task 3.1: Brand Extraction Script
**Priority:** MEDIUM
**Time:** 4 hours

- [ ] Create brand extraction script
  ```bash
  # File: 04_utilities/extract_brands_from_names.py
  ```

- [ ] Build list of known pharmacy brands

- [ ] Implement extraction logic (NLP or regex)

- [ ] Test on sample of 100 products

- [ ] Validate accuracy (manual check)

- [ ] Run on full dataset
  ```sql
  UPDATE canonical_products
  SET brand = extracted_brand
  WHERE brand IS NULL AND extracted_brand IS NOT NULL;
  ```

- [ ] Verify brand coverage ≥ 70%
  ```sql
  SELECT
    COUNT(*) FILTER (WHERE brand IS NOT NULL AND brand != '') as with_brand,
    COUNT(*) as total,
    ROUND(COUNT(*) FILTER (WHERE brand IS NOT NULL AND brand != '')::numeric / COUNT(*) * 100, 1) as pct
  FROM canonical_products
  WHERE is_active = TRUE;
  ```

### Task 3.2: Invalid Barcode Cleanup
**Priority:** LOW
**Time:** 2 hours

- [ ] Analyze 678 invalid barcodes
  ```sql
  SELECT barcode, name, brand, LENGTH(barcode)
  FROM canonical_products
  WHERE LENGTH(barcode) < 8 AND is_active = TRUE
  ORDER BY LENGTH(barcode);
  ```

- [ ] Decide strategy per barcode pattern:
  - Deactivate if clearly invalid
  - Pad with zeros if valid pattern
  - Map to proper EAN/UPC if possible

- [ ] Execute cleanup plan

- [ ] Verify no invalid barcodes remain in active products
  ```sql
  SELECT COUNT(*) FROM canonical_products
  WHERE is_active = TRUE AND LENGTH(barcode) < 8;
  ```

### Task 3.3: Final Validation
**Priority:** CRITICAL
**Time:** 2 hours

- [ ] Run comprehensive data quality check
  ```sql
  -- Should all return 0 or acceptable values
  SELECT 'Missing images' as issue, COUNT(*)
  FROM canonical_products
  WHERE is_active = TRUE AND image_url IS NULL
  UNION ALL
  SELECT 'Missing categories', COUNT(*)
  FROM canonical_products
  WHERE is_active = TRUE AND (category IS NULL OR category = '')
  UNION ALL
  SELECT 'Missing names', COUNT(*)
  FROM canonical_products
  WHERE is_active = TRUE AND (name IS NULL OR name = '')
  UNION ALL
  SELECT 'NULL retailer (active)', COUNT(*)
  FROM canonical_products
  WHERE is_active = TRUE AND source_retailer_id IS NULL;
  ```

- [ ] Verify all active products have pricing
  ```sql
  SELECT COUNT(*) FROM canonical_products cp
  LEFT JOIN retailer_products rp ON cp.barcode = rp.barcode
    AND cp.source_retailer_id = rp.retailer_id
  WHERE cp.is_active = TRUE AND rp.barcode IS NULL;
  ```

- [ ] Check final metrics match expectations
  ```sql
  SELECT
    'Total Active' as metric,
    COUNT(*) as value,
    '40,348' as expected
  FROM canonical_products WHERE is_active = TRUE
  UNION ALL
  SELECT 'Super-Pharm Active',
    COUNT(*),
    '18,882'
  FROM canonical_products WHERE source_retailer_id = 52 AND is_active = TRUE
  UNION ALL
  SELECT 'Good Pharm Active',
    COUNT(*),
    '11,776'
  FROM canonical_products WHERE source_retailer_id = 97 AND is_active = TRUE
  UNION ALL
  SELECT 'Be Pharm Active',
    COUNT(*),
    '9,690'
  FROM canonical_products WHERE source_retailer_id = 150 AND is_active = TRUE;
  ```

- [ ] Generate final data quality report

- [ ] Update DATA_QUALITY_AUDIT_REPORT.md with new metrics

---

## 📊 POST-IMPLEMENTATION VERIFICATION

### Final Checklist
- [ ] Total active products ≥ 40,000
- [ ] Good Pharm active products ≥ 11,500
- [ ] Super-Pharm pricing coverage = 100%
- [ ] All active products have images
- [ ] All active products have categories
- [ ] Brand coverage ≥ 70%
- [ ] No duplicate barcodes per retailer
- [ ] All prices are recent (<30 days)
- [ ] Online store functioning for Super-Pharm
- [ ] Frontend displays all products correctly
- [ ] Search and filtering work properly
- [ ] Price comparison shows online vs. store prices

### Documentation Updates
- [ ] Update README with new data architecture
- [ ] Document scraper schedule and maintenance
- [ ] Create runbook for troubleshooting
- [ ] Update API documentation if endpoints changed

### AI Training Dataset Export
- [ ] Export final cleaned dataset
  ```sql
  COPY (
    SELECT
      cp.barcode, cp.name, cp.brand, cp.category,
      cp.image_url, cp.source_retailer_id,
      p.price, p.price_timestamp, s.storename
    FROM canonical_products cp
    JOIN retailer_products rp ON cp.barcode = rp.barcode
      AND cp.source_retailer_id = rp.retailer_id
    JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
    JOIN stores s ON p.store_id = s.storeid
    WHERE cp.is_active = TRUE
      AND p.price_timestamp >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY cp.source_retailer_id, cp.barcode
  ) TO '/tmp/ai_training_dataset_final.csv' WITH CSV HEADER;
  ```

- [ ] Validate dataset completeness

- [ ] Generate dataset statistics

- [ ] Create data dictionary

- [ ] Ready for AI model training ✅

---

## 🎯 SUCCESS CRITERIA

**All criteria must be met:**

✅ **Quantitative Metrics:**
- Total active products: 40,348 ± 500
- Good Pharm coverage: ≥ 97%
- Super-Pharm pricing: 100%
- Brand coverage: ≥ 70%
- Image accessibility: 100%

✅ **Qualitative Metrics:**
- Data is consistent across retailers
- No obvious errors in product matching
- Pricing data is fresh and reasonable
- Frontend performs well with increased data

✅ **Production Readiness:**
- All automated scrapers running daily
- Monitoring and alerting in place
- Backup and recovery procedures documented
- Dataset ready for AI training

---

## 📝 NOTES & CONSIDERATIONS

### During Implementation:
1. Always test on small samples first
2. Keep backups until verification complete
3. Monitor database performance during large inserts
4. Log all operations for audit trail

### If Issues Arise:
1. Stop immediately
2. Rollback to backup if needed
3. Document the issue
4. Investigate root cause
5. Adjust plan and retry

### Communication:
- Update stakeholders after each phase
- Share progress metrics daily
- Highlight any blockers immediately
- Celebrate wins! 🎉

---

**Document Status:** READY TO EXECUTE
**Last Updated:** October 2, 2025
**Estimated Completion:** October 9, 2025
**Owner:** Data Engineering Team
