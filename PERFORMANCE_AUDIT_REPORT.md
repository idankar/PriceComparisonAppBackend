# PharmMate API Performance Audit Report

**Date:** 2025-10-11
**Auditor:** Claude Code Agent
**Database:** Production PostgreSQL (Render)

---

## Executive Summary

This performance audit identified critical bottlenecks across three key API endpoints. The primary issues stem from **missing database indexes** and **inefficient query patterns** (correlated subqueries). The slowest endpoints are taking **6-7 seconds** to respond, which is unacceptable for a production application.

**Key Findings:**
- `/api/cart/recommendation`: **6.6 seconds** (Critical)
- `/api/deals`: **7.3 seconds** (Critical)
- `/api/products/{product_id}`: **2.4 seconds** (High Priority)

**Root Causes:**
1. Sequential scans on large tables (retailers, canonical_products, promotion_product_links)
2. Correlated subquery executing 404 times in cart recommendation
3. Missing indexes on frequently queried columns
4. External merge sorts using disk instead of memory

**Expected Impact of Recommendations:**
- **80-90% reduction** in response times for all three endpoints
- Target response times: <500ms for product detail, <1s for cart/deals

---

## Detailed Performance Analysis

### 1. `/api/cart/recommendation` Endpoint

**Location:** `02_backend_api/backend.py:1337-1520`
**Current Performance:** 6.6 seconds (6608.479 ms)
**Target Performance:** <1 second

#### EXPLAIN ANALYZE Output

```
Execution Time: 6608.479 ms

Key Operations:
- Seq Scan on retailers r (cost=4305.8ms)
  Filter: retailerid = ANY ('{52,97,150}'::integer[])

- SubPlan 1 (correlated subquery):
  Executed 404 times at 2.951ms each = ~1,200ms total
  Query: SELECT MAX(p2.price_timestamp) FROM prices p2
         WHERE p2.retailer_product_id = p.retailer_product_id
```

#### Performance Anti-Patterns Detected

**CRITICAL: Sequential Scan on retailers (4.3 seconds)**
```
-> Seq Scan on retailers r (actual time=4305.799..4305.803 rows=3 loops=1)
   Filter: (retailerid = ANY ('{52,97,150}'::integer[]))
```
- **Diagnosis:** Database is scanning the entire retailers table to find 3 specific retailer IDs
- **Impact:** 4.3 seconds wasted on a simple lookup
- **Rows Scanned:** All retailers in database
- **Rows Returned:** Only 3 retailers

**CRITICAL: Correlated Subquery (N+1 Problem)**
```
SubPlan 1
  -> Aggregate (cost=599.30..599.31 rows=1 width=8)
     (actual time=2.951..2.951 rows=1 loops=404)
```
- **Diagnosis:** The subquery to find the latest price_timestamp executes 404 times
- **Impact:** ~1.2 seconds of unnecessary repeated work
- **Pattern:** Classic N+1 query problem - should use window function instead

#### Recommended Optimizations

**Priority 1 (Immediate - Deploy Today):**

1. **Add Index on retailers.retailerid**
   ```sql
   CREATE INDEX idx_retailers_retailerid ON retailers (retailerid);
   ```
   - **Expected Impact:** Reduces 4.3s to <10ms (99% reduction)
   - **Justification:** Primary key lookup should be instant

2. **Rewrite Query to Use CTE with Window Function**

   Replace the correlated subquery pattern with:

   ```sql
   WITH latest_prices_by_retailer AS (
       SELECT
           rp.barcode,
           r.retailerid,
           r.retailername,
           p.price,
           ROW_NUMBER() OVER (
               PARTITION BY rp.retailer_product_id
               ORDER BY p.price_timestamp DESC
           ) as rn
       FROM retailer_products rp
       JOIN retailers r ON rp.retailer_id = r.retailerid
       JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
       WHERE rp.barcode IN (...)
         AND r.retailerid = ANY(ARRAY[52, 97, 150])
         AND p.price > 0
   )
   SELECT barcode, retailerid, retailername, price
   FROM latest_prices_by_retailer
   WHERE rn = 1
   ORDER BY barcode, retailerid, price ASC;
   ```

   - **Expected Impact:** Reduces 1.2s to <100ms (90% reduction)
   - **Justification:** Window function runs once instead of 404 times

**Combined Expected Improvement:** 6.6s → <500ms (92% faster)

---

### 2. `/api/deals` Endpoint

**Location:** `02_backend_api/backend.py:737-788`
**Current Performance:** 7.3 seconds (7279.553 ms)
**Target Performance:** <1 second

#### EXPLAIN ANALYZE Output

```
Execution Time: 7279.553 ms

Key Operations:
- Parallel Seq Scan on canonical_products cp (1347.783 ms per worker)
  Filter: is_active AND (lowest_price IS NOT NULL)
  Rows Removed: 32,984 per worker

- Parallel Seq Scan on promotion_product_links ppl (991.200 ms per worker)
  Rows Scanned: 125,016 per worker (250,031 total)

- Seq Scan on promotions p (201.138 ms)
  Filter: (end_date IS NULL) OR (end_date >= NOW())
  Rows Removed: 16,731

- External Merge Sort: Using disk (14112kB + 8848kB)
  Sort Method: external merge
```

#### Performance Anti-Patterns Detected

**CRITICAL: Sequential Scan on canonical_products**
```
-> Parallel Seq Scan on canonical_products cp
   (actual time=0.946..1347.783 rows=21780 loops=2)
   Filter: (is_active AND (lowest_price IS NOT NULL))
   Rows Removed by Filter: 32,984
```
- **Diagnosis:** Scanning entire table and filtering out 60% of rows
- **Impact:** ~1.3 seconds per worker (2.6s total)
- **Missing Index:** `(is_active, lowest_price)`

**CRITICAL: Sequential Scan on promotion_product_links**
```
-> Parallel Seq Scan on promotion_product_links ppl
   (actual time=0.673..991.200 rows=125,016 loops=2)
```
- **Diagnosis:** Reading entire 250k row table
- **Impact:** ~1 second per worker (2s total)
- **Missing Index:** `promotion_id` for the join

**HIGH: Sequential Scan on promotions**
```
-> Seq Scan on promotions p (actual time=0.313..201.138 rows=2,358 loops=2)
   Filter: (end_date IS NULL) OR (end_date >= NOW())
   Rows Removed by Filter: 16,731
```
- **Diagnosis:** Filtering out 87% of rows
- **Impact:** ~200ms per worker (400ms total)
- **Missing Index:** `end_date`

**MEDIUM: External Merge Sort**
```
Sort Method: external merge  Disk: 14112kB
Worker 0: Sort Method: external merge  Disk: 8848kB
```
- **Diagnosis:** Result set too large for work_mem, spilling to disk
- **Impact:** Disk I/O adds latency

#### Recommended Optimizations

**Priority 1 (Immediate - Deploy Today):**

3. **Add Composite Index on canonical_products**
   ```sql
   CREATE INDEX idx_canonical_products_active_price
   ON canonical_products (is_active, lowest_price)
   WHERE is_active = true AND lowest_price IS NOT NULL;
   ```
   - **Expected Impact:** Reduces 2.6s to <200ms (92% reduction)
   - **Justification:** Partial index perfect for this filter pattern

4. **Add Index on promotion_product_links.promotion_id**
   ```sql
   CREATE INDEX idx_promotion_product_links_promotion_id
   ON promotion_product_links (promotion_id);
   ```
   - **Expected Impact:** Reduces 2s to <100ms (95% reduction)
   - **Justification:** Enables efficient join instead of full table scan

**Priority 2 (This Week):**

5. **Add Index on promotions.end_date**
   ```sql
   CREATE INDEX idx_promotions_end_date
   ON promotions (end_date)
   WHERE end_date IS NULL OR end_date >= NOW();
   ```
   - **Expected Impact:** Reduces 400ms to <50ms (87% reduction)
   - **Justification:** Partial index for active promotions only

6. **Consider Alternative to RANDOM() Ordering**

   Current approach prevents LIMIT pushdown optimization. Alternative:

   ```sql
   -- Option A: Use TABLESAMPLE for faster random sampling
   SELECT * FROM (
       SELECT DISTINCT ON (p.promotion_id) ...
       FROM promotions p TABLESAMPLE SYSTEM (10)
       ...
   ) ORDER BY deal_id LIMIT 50;

   -- Option B: Pre-select random promotion IDs first
   WITH random_promotions AS (
       SELECT promotion_id
       FROM promotions
       WHERE (end_date IS NULL OR end_date >= NOW())
       ORDER BY RANDOM()
       LIMIT 50
   )
   SELECT ...
   FROM random_promotions rp
   JOIN promotions p ON p.promotion_id = rp.promotion_id
   ...
   ```

   - **Expected Impact:** Reduces sort overhead by 50-70%

**Combined Expected Improvement:** 7.3s → <800ms (89% faster)

---

### 3. `/api/products/{product_id}` Endpoint

**Location:** `02_backend_api/backend.py:655-735`
**Current Performance:** 2.4 seconds (2387.210 ms)
**Target Performance:** <300ms

#### EXPLAIN ANALYZE Output

```
Execution Time: 2387.210 ms

Key Operations:
- Seq Scan on canonical_products cp
  Filter: is_active AND ((barcode)::text = '7290018104941'::text)
  Rows Removed: 109,526

- SubPlan 2 (promotions subquery):
  Seq Scan on promotion_product_links ppl (389.984 ms × 2 loops)
  Rows Scanned: 250,031
```

#### Performance Anti-Patterns Detected

**CRITICAL: Sequential Scan on canonical_products**
```
Seq Scan on canonical_products cp (actual time=711.065..2301.686 rows=2 loops=1)
  Filter: (is_active AND ((barcode)::text = '7290018104941'::text))
  Rows Removed by Filter: 109,526
```
- **Diagnosis:** Scanning entire 109k row table to find 1 product by barcode
- **Impact:** ~1.5 seconds wasted
- **Missing Index:** `barcode` column (with is_active)

**CRITICAL: Sequential Scan on promotion_product_links (in SubPlan)**
```
SubPlan 2
  -> Seq Scan on promotion_product_links ppl
     (actual time=0.373..389.984 rows=250,031 loops=2)
```
- **Diagnosis:** Subquery scans entire table twice (loops=2)
- **Impact:** ~800ms (390ms × 2)
- **Missing Index:** `retailer_product_id`

#### Recommended Optimizations

**Priority 1 (Immediate - Deploy Today):**

7. **Add Index on canonical_products.barcode**
   ```sql
   CREATE INDEX idx_canonical_products_barcode
   ON canonical_products (barcode)
   WHERE is_active = true;
   ```
   - **Expected Impact:** Reduces 1.5s to <10ms (99% reduction)
   - **Justification:** Direct barcode lookup should be instant

8. **Add Index on promotion_product_links.retailer_product_id**
   ```sql
   CREATE INDEX idx_promotion_product_links_retailer_product_id
   ON promotion_product_links (retailer_product_id);
   ```
   - **Expected Impact:** Reduces 800ms to <50ms (94% reduction)
   - **Justification:** Enables index-based join in subquery

**Good News:** The CTE with window function for `latest_prices` is already optimized (only 6.7ms).

**Combined Expected Improvement:** 2.4s → <200ms (92% faster)

---

## Prioritized Action Plan

### Phase 1: Critical Fixes (Deploy Immediately - Today)

**Total Expected Time Savings: ~15 seconds across all endpoints**

Execute these DDL statements on production database:

```sql
-- Fix #1: Cart Recommendation - retailers lookup (saves 4.3s)
CREATE INDEX idx_retailers_retailerid ON retailers (retailerid);

-- Fix #2: Product Detail - barcode lookup (saves 1.5s)
CREATE INDEX idx_canonical_products_barcode
ON canonical_products (barcode)
WHERE is_active = true;

-- Fix #3: Deals - canonical products filter (saves 2.6s)
CREATE INDEX idx_canonical_products_active_price
ON canonical_products (is_active, lowest_price)
WHERE is_active = true AND lowest_price IS NOT NULL;

-- Fix #4: Deals & Product Detail - promotion links (saves 2.8s)
CREATE INDEX idx_promotion_product_links_promotion_id
ON promotion_product_links (promotion_id);

CREATE INDEX idx_promotion_product_links_retailer_product_id
ON promotion_product_links (retailer_product_id);

-- Fix #5: Deals - promotions filter (saves 400ms)
CREATE INDEX idx_promotions_end_date
ON promotions (end_date)
WHERE end_date IS NULL OR end_date >= NOW();

-- Analyze tables after creating indexes
ANALYZE retailers;
ANALYZE canonical_products;
ANALYZE promotion_product_links;
ANALYZE promotions;
```

**Deployment Instructions:**
1. Run during low-traffic period (these will lock tables briefly)
2. Monitor index creation progress: `SELECT * FROM pg_stat_progress_create_index;`
3. Expected index creation time: 2-5 minutes per index
4. Verify indexes created: `\d+ table_name`

### Phase 2: Query Rewrites (Deploy This Week)

**Backend Code Changes Required:**

**1. Rewrite `/api/cart/recommendation` query** (`backend.py:1389-1407`)

Replace the existing query with the optimized CTE version:

```python
# NEW OPTIMIZED QUERY
query = f"""
    WITH latest_prices_by_retailer AS (
        SELECT
            rp.barcode,
            r.retailerid,
            r.retailername,
            p.price,
            ROW_NUMBER() OVER (
                PARTITION BY rp.retailer_product_id
                ORDER BY p.price_timestamp DESC
            ) as rn
        FROM retailer_products rp
        JOIN retailers r ON rp.retailer_id = r.retailerid
        JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
        WHERE rp.barcode IN ({placeholders})
          AND r.retailerid = ANY(%s)
          AND p.price > 0
    )
    SELECT barcode, retailerid, retailername, price
    FROM latest_prices_by_retailer
    WHERE rn = 1
    ORDER BY barcode, retailerid, price ASC;
"""
```

**Expected Impact:** Eliminates correlated subquery, saves 1.2 seconds

**2. (Optional) Optimize `/api/deals` RANDOM() ordering** (`backend.py:754-783`)

Consider using `TABLESAMPLE` for better performance on large result sets.

### Phase 3: Database Tuning (Optional - This Month)

**PostgreSQL Configuration Adjustments:**

```sql
-- Increase work_mem to avoid external merge sorts
ALTER DATABASE pharmmate_db_production SET work_mem = '16MB';  -- Default is often 4MB

-- Increase shared_buffers if you have available RAM
ALTER SYSTEM SET shared_buffers = '256MB';  -- Adjust based on your instance

-- Enable parallel query workers for large scans
ALTER DATABASE pharmmate_db_production SET max_parallel_workers_per_gather = 2;

-- Reload configuration
SELECT pg_reload_conf();
```

**Note:** Consult with Render support before changing system-wide settings.

---

## Validation & Monitoring

### Post-Deployment Verification

After deploying Phase 1 indexes, verify performance improvements:

```sql
-- Re-run EXPLAIN ANALYZE on each query to confirm improvements

-- 1. Cart Recommendation Query
EXPLAIN ANALYZE
SELECT ...  -- (use the same test query from audit)

-- 2. Deals Query
EXPLAIN ANALYZE
SELECT ...  -- (use the same test query from audit)

-- 3. Product Detail Query
EXPLAIN ANALYZE
WITH latest_prices AS ...  -- (use the same test query from audit)
```

**Success Criteria:**
- No "Seq Scan" on retailers, canonical_products, or promotion_product_links
- All scans should show "Index Scan" or "Index Only Scan"
- Execution times should be <1 second for all queries

### Ongoing Monitoring

**Key Metrics to Track:**

```sql
-- Query 1: Find slow queries in production
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
WHERE mean_exec_time > 1000  -- Queries averaging >1 second
ORDER BY mean_exec_time DESC
LIMIT 10;

-- Query 2: Check index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

-- Query 3: Identify missing indexes (tables with high seq scans)
SELECT
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    seq_tup_read / seq_scan as avg_seq_read
FROM pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC
LIMIT 10;
```

**Set up alerts for:**
- API endpoint response times >2 seconds
- Database query execution times >1 second
- Sequential scans on large tables (>10k rows)

---

## Risk Assessment

### Index Creation Risks

**Low Risk:**
- All recommended indexes are **non-blocking** (use default CREATE INDEX)
- Tables will be locked briefly during creation (~2-5 minutes)
- No data modification, only read performance improvements

**Mitigation:**
- Deploy during off-peak hours
- Create indexes one at a time
- If a lock timeout occurs, retry during even lower traffic period

### Query Rewrite Risks

**Medium Risk:**
- Code changes could introduce bugs
- Window function CTE changes query semantics slightly

**Mitigation:**
- Test query rewrites in staging environment first
- Add integration tests for cart recommendation endpoint
- Deploy with feature flag to allow quick rollback
- Monitor error rates closely after deployment

---

## Expected Outcomes

### Performance Improvements Summary

| Endpoint | Current | Target | Improvement |
|----------|---------|--------|-------------|
| `/api/cart/recommendation` | 6.6s | <500ms | **92% faster** |
| `/api/deals` | 7.3s | <800ms | **89% faster** |
| `/api/products/{product_id}` | 2.4s | <200ms | **92% faster** |

### User Experience Impact

**Before:**
- Cart recommendation: 6-7 second wait (unacceptable)
- Deals page: 7+ second load time (users likely abandon)
- Product detail: 2-3 second delay (frustrating)

**After:**
- Cart recommendation: <500ms (feels instant)
- Deals page: <1 second (acceptable)
- Product detail: <300ms (smooth, responsive)

### Business Impact

**Projected Improvements:**
- **Reduced bounce rate** on deals page (currently loading too slowly)
- **Increased cart conversions** (faster recommendation = better UX)
- **Lower server costs** (queries using indexes are 10-100x more efficient)
- **Improved SEO** (Google penalizes slow-loading pages)

---

## Appendix: Technical Details

### A. Database Schema Assumptions

Based on the query analysis, the current schema structure:

```
canonical_products (109,528 rows)
  - barcode (text) [MISSING INDEX]
  - is_active (boolean)
  - lowest_price (numeric)
  - name, brand, image_url

retailers (~6 rows)
  - retailerid (integer) [MISSING INDEX]
  - retailername (text)

retailer_products
  - retailer_product_id (PK)
  - barcode (text) [HAS INDEX: idx_retailer_products_barcode]
  - retailer_id (FK)

prices (8,600,000+ rows)
  - price_id (PK)
  - retailer_product_id (FK) [HAS INDEX: idx_prices_retailer_product_id]
  - price_timestamp (timestamp)
  - price (numeric)
  - store_id (FK)

promotions (~19,089 rows total, ~2,358 active)
  - promotion_id (PK)
  - retailer_id (FK)
  - end_date (timestamp) [MISSING INDEX]
  - description, remarks

promotion_product_links (250,031 rows)
  - promotion_id (FK) [MISSING INDEX]
  - retailer_product_id (FK) [MISSING INDEX]

stores
  - storeid (PK)
  - retailerid (FK)
  - isactive (boolean)
```

### B. PostgreSQL Version

Assumed version: PostgreSQL 12+ (based on EXPLAIN ANALYZE output features like parallel workers, window functions, CTEs).

### C. Index Size Estimates

Approximate disk space required for new indexes:

```sql
-- Estimate index sizes (run after creation)
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE indexname LIKE 'idx_%'
ORDER BY pg_relation_size(indexrelid) DESC;
```

Expected sizes:
- `idx_retailers_retailerid`: <100 KB (tiny table)
- `idx_canonical_products_barcode`: ~5-10 MB
- `idx_canonical_products_active_price`: ~3-5 MB (partial index)
- `idx_promotion_product_links_promotion_id`: ~10-15 MB
- `idx_promotion_product_links_retailer_product_id`: ~10-15 MB
- `idx_promotions_end_date`: ~1-2 MB (partial index)

**Total additional storage:** ~40-60 MB (negligible)

---

## Conclusion

This audit identified **critical performance bottlenecks** caused by missing indexes and inefficient query patterns. The recommended fixes are **low-risk, high-impact** changes that will reduce API response times by **89-92%**.

**Next Steps:**
1. Review and approve this report
2. Schedule Phase 1 deployment (index creation) for off-peak hours today
3. Test Phase 2 query rewrites in staging this week
4. Deploy Phase 2 by end of week
5. Monitor performance metrics and validate improvements

**Contact:** For questions or deployment assistance, please reach out.

---

**Report Generated:** 2025-10-11
**Last Updated:** 2025-10-11
**Version:** 1.0
