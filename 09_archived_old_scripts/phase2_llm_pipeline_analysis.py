"""
Phase 2: LLM Pipeline Analysis
Simulates Claude Haiku extraction of retailer URLs from Google search results
"""

# Define our target retailers from Phase 1
TARGET_RETAILERS = [
    "iherb.com",
    "il.iherb.com",
    "strawberrynet.com",
    "caretobeauty.com",
    "cultbeauty.com",
    "lookfantastic.com",
    "cosmostore.org",
    "il.cosmostore.org",
    "makeupstore.co.il"
]

# Test products and their search results
test_results = [
    {
        "product_name": "סלפאק - ממחטות בקופסא 100 יח'",
        "brand": "סלפאק",
        "barcode": "8690530036727",
        "search_results": {
            "product_identified": "Selpak Maxi tissues 100 pieces",
            "retailers_found": [
                {"name": "olivia.az", "url": "https://olivia.az/salfet-qutuda-selpak-maxi-8690530036727"},
                {"name": "pazarama.com", "url": "https://www.pazarama.com/selpak-prof-kutu-mendil-50li-18x21-p-8690530036789"},
                {"name": "amazon.in", "url": "https://www.amazon.in/Selpak-Facial-Tissue-Refill-200pulls/dp/B08VS68W54"}
            ],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "לייף מארז משולב בדיקות ביוץ + בידקת הריון + יחידה",
        "brand": "לייף",
        "barcode": "7290109443713",
        "search_results": {
            "product_identified": "FAB Defense Tavor Bipod (INCORRECT - barcode mismatch)",
            "retailers_found": [
                {"name": "ebay.com", "url": "https://www.ebay.com/p/2120937470"}
            ],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "איירבוריאן CC WATER קרם בגוון בהיר",
        "brand": "General",
        "barcode": "8809255786071",
        "search_results": {
            "product_identified": "Erborian CC Water Fresh Complexion Gel CLAIR 40ml",
            "retailers_found": [
                {"name": "pharmacie-cap3000.com", "url": "https://www.pharmacie-cap3000.com/soins-du-visage/136858-erborian-cc-water-clair-40ml-8809255786071.html"},
                {"name": "ebay.com", "url": "https://www.ebay.com/itm/155202842361"},
                {"name": "e.leclerc", "url": "https://www.e.leclerc/fp/cc-water-claire-40ml-8809255786071"}
            ],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "Keeeper סיר גמילה - כוכבים כחול",
        "brand": "Keeeper סיר גמילה",
        "barcode": "5060299502604",
        "search_results": {
            "product_identified": "NOT FOUND",
            "retailers_found": [],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "ברט - מיני אובלטים עם קימל 225 גרם",
        "brand": "ברט",
        "barcode": "7290119860593",
        "search_results": {
            "product_identified": "NOT CLEARLY IDENTIFIED (Stumble Guys toys appeared instead)",
            "retailers_found": [],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "גייקובס קרוננג קפה נמס מיובש בהקפאה 200ג",
        "brand": "דיפלומט",
        "barcode": "8714599513866",
        "search_results": {
            "product_identified": "Jacobs Kronung instant coffee 200g",
            "retailers_found": [
                {"name": "chp.co.il", "url": "https://chp.co.il/חיפה/9000/4000/קפה+נמס+ג'ייקובס+קרונונג+מיובש"},
                {"name": "shophoms.co.il", "url": "https://shophoms.co.il/product/ג'ייקובס-קרונונג-קפה-נמס-צנ-200ג/"},
                {"name": "shufersal.co.il", "url": "https://www.shufersal.co.il/online/he/p/P_8714599513866"},
                {"name": "rami-levy.co.il", "url": "https://www.rami-levy.co.il/he/online/market/..."},
                {"name": "ebay.com", "url": "https://www.ebay.com/p/764530697"}
            ],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "סליידר - מסרק לשיער גדול",
        "brand": "סליידר",
        "barcode": "7290109207797",
        "search_results": {
            "product_identified": "NOT CLEARLY IDENTIFIED",
            "retailers_found": [],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "לייף זוג רצועות שעווה לגוף אלוורה ארגן16",
        "brand": "זר הייטק",
        "barcode": "7290113890558",
        "search_results": {
            "product_identified": "NOT CLEARLY IDENTIFIED",
            "retailers_found": [],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "פיניש אולטימייט פלוס - רגיל 48 קפסולות",
        "brand": "רקיט בנקיזר",
        "barcode": "8002910063344",
        "search_results": {
            "product_identified": "Finish Ultimate Plus dishwasher detergent 48 tablets",
            "retailers_found": [
                {"name": "filgistore.it", "url": "https://www.filgistore.it/en/detersivo-lavastoviglie/721-finish-powerball-ultimate-plus"},
                {"name": "agrariagioiese.it", "url": "https://agrariagioiese.it/en/dishwasher/finish-ultimate-plus"},
                {"name": "thanopoulos.gr", "url": "https://www.thanopoulos.gr/en/products/finish-powerball-ultimate-plus"}
            ],
            "target_retailers_found": []
        }
    },
    {
        "product_name": "פראדה קנדי א.אדפ 80מ\"ל",
        "brand": "פראדה בישום",
        "barcode": "8435137727087",
        "search_results": {
            "product_identified": "Prada Candy Eau de Parfum 2.7 oz / 80ml",
            "retailers_found": [
                {"name": "worldofwatches.com", "url": "https://www.worldofwatches.com/pid/80883/prada-candy-prada-edp-spray-2-7-oz-w-cos-pcaes27"},
                {"name": "jomashop.com", "url": "https://www.jomashop.com/prada-perfume-pcaes27.html"},
                {"name": "prada-beauty.com", "url": "https://www.prada-beauty.com/fragrance/candy/candy-eau-de-parfum/8435137727087.html"},
                {"name": "perfumeheadquarters.com", "url": "https://perfumeheadquarters.com/products/prada-candy-eau-de-parfum-spray-for-women"}
            ],
            "target_retailers_found": []
        }
    }
]

# Analysis
print("=" * 80)
print("PHASE 2: LLM PIPELINE TEST RESULTS")
print("=" * 80)

successful_identifications = 0
target_retailer_matches = 0
total_products = len(test_results)

for i, result in enumerate(test_results, 1):
    print(f"\nProduct #{i}:")
    print(f"  Name: {result['product_name']}")
    print(f"  Brand: {result['brand']}")
    print(f"  Barcode: {result['barcode']}")
    print(f"  Identified As: {result['search_results']['product_identified']}")
    print(f"  Retailers Found: {len(result['search_results']['retailers_found'])}")
    print(f"  Target Retailers Matched: {len(result['search_results']['target_retailers_found'])}")

    if result['search_results']['product_identified'] not in ["NOT FOUND", "NOT CLEARLY IDENTIFIED"] and \
       "INCORRECT" not in result['search_results']['product_identified']:
        successful_identifications += 1

    if result['search_results']['target_retailers_found']:
        target_retailer_matches += 1

print("\n" + "=" * 80)
print("SUMMARY STATISTICS:")
print("=" * 80)
print(f"Total Products Tested: {total_products}")
print(f"Successfully Identified: {successful_identifications} ({successful_identifications/total_products*100:.1f}%)")
print(f"Found on Target Retailers: {target_retailer_matches} ({target_retailer_matches/total_products*100:.1f}%)")
print(f"\nKey Finding: Barcode-only search does NOT reliably find products on target retailers")
print(f"Recommendation: Need to enhance search with product name + brand for better results")
