# DATA QUALITY AUDIT REPORT
## canonical_products Table Analysis for AI Visual Recognition Training

**Date:** October 2, 2025
**Analyst:** Claude (Data Scientist Mode)
**Purpose:** Pre-training data quality assessment for AI visual recognition model

---

## EXECUTIVE SUMMARY

### ⚠️ CRITICAL VERDICT: DATA IS **NOT PRODUCTION-READY**

The canonical_products table contains **significant data quality issues** that will severely impact AI model training. While image coverage is good (100% for active products), there are major gaps in:
1. **Product-Pricing alignment** (46.4% of Super-Pharm products lack pricing)
2. **Data completeness** (massive coverage gaps vs. source data)
3. **Brand information** (66.7% missing)
4. **Data integrity** (orphaned records, invalid barcodes)

---

## TABLE OVERVIEW

### Active Products Summary
- **Total Active Products:** 28,815
- **Total Products (including inactive):** 45,615
- **Unique Barcodes:** 45,615 (no duplicates ✓)
- **Products with Images:** 29,121 (100% of active products ✓)
- **Products with Categories:** 31,739 (100% of active products ✓)
- **Products with Brands:** 25,644 (only 33.3% of active products ⚠️)

---

## RETAILER BREAKDOWN

### Super-Pharm (Retailer ID: 52)
- **Total Products:** 24,896
- **Active Products:** 18,882 (75.8%)
- **Inactive Products:** 6,014
- **Image Coverage:** 18,882/18,882 active (100% ✓)
- **Category Coverage:** 18,882/18,882 active (100% ✓)
- **Brand Coverage:** 13,358/18,882 active (70.7%)

#### **CRITICAL ISSUE:** Pricing Data Coverage
- **Products in retailer_products:** 19,621
- **Active products with pricing:** 10,120
- **Active products WITHOUT pricing:** 9,590 (50.8% ⚠️⚠️⚠️)
- **Pricing Coverage:** **53.6%** - UNACCEPTABLE for production

#### XML Files Processed
- **Files:** 938 XML files successfully processed
- **Rows Added:** 7,164,854 pricing records
- **Last Price Update:** September 20, 2025

#### Data Source Analysis
- **Images:** Azure Blob Storage (all 18,882 from superpharmstorage.blob.core.windows.net)
- **Pricing:** Government XML transparency portal
- **Product Data:** Mixed (XML + commercial scraper)

### Be Pharm (Retailer ID: 150)
- **Total Products:** 9,712
- **Active Products:** 9,690 (99.8%)
- **Inactive Products:** 22
- **Image Coverage:** 9,690/9,690 active (100% ✓)
- **Category Coverage:** 9,690/9,690 active (100% ✓)
- **Brand Coverage:** 1,300/9,690 active (13.4% ⚠️)

#### Pricing Data Coverage
- **Products in retailer_products:** 9,315
- **Active products with pricing:** 8,662
- **Active products WITHOUT pricing:** 1,028 (10.6%)
- **Pricing Coverage:** **89.4%** - Acceptable but not perfect

#### **INTERESTING FINDING:** Be Pharm Data Source
- **XML Files Processed:** 0 (NONE!)
- **Images:** ALL from Shufersal's Cloudinary (res.cloudinary.com/shufersal)
- **Explanation:** Be Pharm scraper downloads from Shufersal portal (ChainId 7290027600007)
- **Last Price Update:** October 2, 2025 (most recent)
- **Implication:** Be Pharm products are actually Shufersal products filtered by chain ID

#### Invalid Barcodes
- **Products with <8 digit barcodes:** 678 (all from Be Pharm)
- **Example:** 442266, 1066218, 1471869, etc. (6-7 digits)
- **Impact:** These are likely internal SKUs, not proper EAN/UPC barcodes

### Good Pharm (Retailer ID: 97)
- **Total Products:** 375
- **Active Products:** 243 (64.8%)
- **Inactive Products:** 132
- **Image Coverage:** 243/243 active (100% ✓)
- **Category Coverage:** 243/243 active (100% ✓)
- **Brand Coverage:** 375/243 active (>100% - includes inactive)

#### **CRITICAL ISSUE:** Massive Coverage Gap
- **Products in retailer_products:** 12,134
- **Active products in canonical:** 243
- **Products with pricing NOT in canonical:** 11,776 (97% missing! ⚠️⚠️⚠️)
- **Implication:** Only 2% of Good Pharm's actual inventory is represented in active canonical products

#### XML Files Processed
- **Files:** 624 XML files successfully processed
- **Rows Added:** 1,457,084 pricing records
- **Last Price Update:** September 25, 2025

#### Data Source
- **Images:** goodpharm.co.il (proper source)
- **Pricing:** Government XML transparency portal
- **Coverage:** Extremely low - commercial scraper is barely functional

---

## DATA QUALITY ISSUES

### 1. Orphaned Data (Critical)
- **Products with NULL source_retailer_id:** 10,632 (all inactive)
- **Active products without retailer_products entry:** 10,635
- **Impact:** These products exist in the catalog but have no pricing or retailer linkage

### 2. Brand Data (High Priority)
- **Active products with brands:** 9,582 (33.3%)
- **Active products WITHOUT brands:** 19,233 (66.7% ⚠️)
- **Impact:** Brand is a critical feature for visual recognition - this gap is significant

### 3. Barcode Quality
- **Valid EAN-13 (13 digits):** Need to check
- **Valid EAN-8 (8 digits):** Need to check
- **Valid UPC-A (12 digits):** Need to check
- **Invalid/Short barcodes (<8 digits):** 678
- **Impact:** Invalid barcodes cannot be matched to retail systems

### 4. Category Structure
- **Super-Pharm:** Deep hierarchy (up to 4 levels: טיפוח/טיפוח שיער/צבע לשיער/etc)
- **Be Pharm:** Mixed hierarchy (mostly single-level, some deep)
- **Good Pharm:** Simple single-level categories
- **Impact:** Inconsistent category schemas across retailers - difficult to train unified model

### 5. Data Freshness
- **Super-Pharm prices:** Last updated September 20, 2025 (12 days old)
- **Be Pharm prices:** Last updated October 2, 2025 (TODAY ✓)
- **Good Pharm prices:** Last updated September 25, 2025 (7 days old)

---

## IMAGE QUALITY ASSESSMENT

### Accessibility Test (Random Sample of 30)
- **Status:** ✓ All tested images returned HTTP 200
- **Content-Type:** Proper image types (image/jpeg, image/png)
- **SSL:** All HTTPS (secure ✓)

### Image Sources
1. **Super-Pharm:** Azure CDN (superpharmstorage.blob.core.windows.net)
   - Format: JPEG
   - Path: /hybris/products/desktop/small/{barcode}.jpg
   - Quality: Likely consistent (standardized Azure storage)

2. **Be Pharm:** Shufersal Cloudinary
   - Format: PNG
   - Path: res.cloudinary.com/shufersal/.../products_large/{SKU}_L_P_{barcode}_1.png
   - Quality: Likely good (Cloudinary CDN)
   - **Note:** These are Shufersal's product images, not Be Pharm's own images

3. **Good Pharm:** Direct website
   - Format: JPEG
   - Path: goodpharm.co.il/wp-content/uploads/YYYY/MM/{barcode}_SN-250x250.jpg
   - Size: 250x250px (small!)
   - Quality: Likely variable (WordPress uploads)

---

## SOURCE DATA COMPARISON

### XML Files vs. Active Products

#### Super-Pharm
- **XML pricing records:** 7,164,854
- **Unique products in retailer_products:** 19,621
- **Active canonical products:** 18,882
- **Gap:** 740 products have pricing but are inactive
- **Bigger Gap:** ~9,500 products have pricing but no active canonical entry

#### Good Pharm
- **XML pricing records:** 1,457,084
- **Unique products in retailer_products:** 12,134
- **Active canonical products:** 243
- **Gap:** **11,891 products missing** (97.9% of inventory not in active catalog!)

#### Be Pharm
- **XML pricing records:** 0 (uses Shufersal portal)
- **Unique products in retailer_products:** 9,315
- **Active canonical products:** 9,690
- **Gap:** None (actually 103.9% coverage - likely includes some inactive)

---

## CRITICAL FINDINGS FOR AI TRAINING

### ✅ What's Good
1. **No duplicate barcodes** - Data integrity at barcode level is solid
2. **100% image coverage for active products** - Every active product has an image
3. **100% category coverage for active products** - All products are categorized
4. **Images are accessible** - All tested image URLs return valid images
5. **Be Pharm data freshness** - Updated today

### ⚠️ Major Concerns

#### 1. **Incomplete Product Coverage** (CRITICAL)
Your active catalog represents only a **fraction** of available products:
- **Good Pharm:** Only 2% of available products (243 out of ~12,000)
- **Super-Pharm:** Only 48% of available products (18,882 out of ~39,500 if you count the missing 9,501)

**Impact on AI Training:**
- Model will only learn to recognize a small subset of products
- Cannot generalize to full pharmacy inventory
- Will fail to recognize ~98% of Good Pharm products in real scenarios

#### 2. **Missing Pricing Data for Active Products** (CRITICAL)
- **Super-Pharm:** 9,590 active products (50.8%) have NO current pricing
- **Implication:** These products are "active" but not actually sellable

**Impact on AI Training:**
- Unclear which products are truly available for purchase
- Training on products that may not be in active retail circulation
- Price is a critical feature for many use cases - half your data lacks it

#### 3. **Missing Brand Information** (HIGH)
- **66.7% of active products** have no brand information
- Brands are critical for visual recognition (packaging often prominently displays brand)

**Impact on AI Training:**
- Cannot use brand as a classification feature
- Harder to distinguish between similar products from different brands
- Lower model accuracy expected

#### 4. **Inconsistent Data Sources**
- **Super-Pharm:** Government XML + Azure images
- **Be Pharm:** Shufersal portal (masquerading as Be Pharm)
- **Good Pharm:** Government XML + website scraper (barely working)

**Impact on AI Training:**
- Data quality inconsistency across retailers
- Be Pharm data may not reflect actual Be Pharm stores (it's Shufersal data!)
- Image quality/style varies significantly

#### 5. **Invalid Barcodes** (MEDIUM)
- **678 products** have invalid barcodes (<8 digits)
- All from Be Pharm
- Likely internal SKUs rather than scannable barcodes

**Impact on AI Training:**
- Cannot validate these products in real retail scenarios
- May be duplicate entries under different SKU systems

---

## RECOMMENDATIONS

### Before AI Training (MUST DO)

1. **Fix Good Pharm Coverage**
   - Current: 243 active products
   - Available: ~12,000 products
   - **Action:** Investigate and repair Good Pharm commercial scraper
   - **Impact:** Would increase dataset by ~11,800 products

2. **Fix Super-Pharm Pricing Coverage**
   - Current: 53.6% of active products have pricing
   - **Action:** Investigate why 9,590 active products have no retailer_products entry
   - **Options:**
     a. Deactivate products without pricing (reduce dataset to 10,120)
     b. Run comprehensive pricing backfill from XML files
   - **Recommended:** Option B - backfill pricing data

3. **Address Be Pharm Data Source**
   - **Issue:** Be Pharm products are actually Shufersal products
   - **Question:** Is this intentional? Does Be Pharm use Shufersal's product database?
   - **Action:** Verify this is correct business logic, not a data error

4. **Brand Data Backfill**
   - Current: 33.3% coverage
   - **Action:** Extract brand from product names using NLP
   - **Example:** "פרנואר שמפו" → Brand: "פרנואר"
   - **Impact:** Could improve coverage to 70-80%

5. **Clean Invalid Barcodes**
   - **Action:** Investigate 678 products with <8 digit barcodes
   - **Options:**
     a. Deactivate if they're invalid
     b. Pad with zeros if they're valid but short
     c. Map to proper EAN/UPC if they're internal SKUs

### Dataset Composition Options

#### Option A: Current State (NOT RECOMMENDED)
- **Size:** 28,815 products
- **Completeness:** ~40% of available inventory
- **Pricing Coverage:** 67% overall, 53.6% for Super-Pharm
- **Verdict:** INCOMPLETE - will produce poor model

#### Option B: Conservative (Products with Pricing Only)
- **Size:** ~19,180 products (9,292 Super-Pharm + 8,662 Be Pharm + 226 Good Pharm)
- **Completeness:** ~19% of available inventory
- **Pricing Coverage:** 100%
- **Verdict:** SMALL but COMPLETE - safe for training but limited scope

#### Option C: Ideal (After Fixes)
- **Size:** ~33,000+ products (after Good Pharm fix + pricing backfill)
- **Completeness:** ~50%+ of available inventory
- **Pricing Coverage:** >95%
- **Brand Coverage:** >70% (after brand extraction)
- **Verdict:** READY FOR PRODUCTION

---

## FINAL VERDICT

### Current State: **NOT PRODUCTION-READY** ❌

**Reasons:**
1. Massive coverage gaps (only 2% of Good Pharm inventory)
2. Half of Super-Pharm products lack pricing data
3. Be Pharm data source is questionable (Shufersal, not Be Pharm)
4. 66.7% missing brand data
5. 678 invalid barcodes

### Minimum Required Actions Before Training:

**Critical (Must Fix):**
- [ ] Fix Good Pharm scraper and activate remaining ~11,800 products
- [ ] Backfill pricing data for 9,590 Super-Pharm products
- [ ] Verify Be Pharm data source correctness

**High Priority (Should Fix):**
- [ ] Extract brand names from product names (NLP-based)
- [ ] Clean/validate 678 invalid barcodes
- [ ] Verify image quality across all sources

**Medium Priority (Nice to Have):**
- [ ] Standardize category schemas across retailers
- [ ] Add product descriptions if available
- [ ] Verify all products are actually available in stores

---

## DATA EXPORT RECOMMENDATION

### If You Must Train Now (Not Recommended)

Export only products with complete data:
```sql
SELECT *
FROM canonical_products cp
JOIN retailer_products rp ON cp.barcode = rp.barcode AND cp.source_retailer_id = rp.retailer_id
JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
WHERE cp.is_active = TRUE
  AND cp.image_url IS NOT NULL
  AND cp.category IS NOT NULL
  AND cp.brand IS NOT NULL
  AND p.price_timestamp >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY cp.source_retailer_id, cp.barcode;
```

This would give you ~9,000 high-quality products with complete data.

### Recommended: Wait and Fix

Fix the issues above first, then export the full clean dataset of ~33,000+ products.

---

**Report Generated:** October 2, 2025
**Next Review:** After data quality fixes implemented
