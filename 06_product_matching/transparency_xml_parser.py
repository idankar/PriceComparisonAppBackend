#!/usr/bin/env python3
"""
Government Transparency XML Parser with Barcode Extraction
Properly parses XML files to extract ItemCode (EAN/Barcode) for better matching
"""

import xml.etree.ElementTree as ET
import psycopg2
import json
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TransparencyProduct:
    item_code: str  # This is the EAN/Barcode!
    item_name: str
    manufacturer_name: str
    manufacturer_description: str
    price: float
    quantity: float
    unit_qty: str
    unit_of_measure: str
    price_update_date: str
    store_id: str
    chain_id: str

class TransparencyXMLParser:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="price_comparison_app_v2",
            user="postgres",
            password="025655358"
        )
    
    def parse_xml_file(self, xml_path: str) -> List[TransparencyProduct]:
        """Parse XML file and extract all products with barcodes"""
        products = []
        
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # Extract store info
            envelope = root.find('Envelope')
            chain_id = envelope.find('ChainId').text if envelope.find('ChainId') is not None else ''
            store_id = envelope.find('StoreId').text if envelope.find('StoreId') is not None else ''
            
            # Extract all product lines
            lines = root.findall('.//Line')
            
            for line in lines:
                try:
                    # Extract all fields
                    item_code = line.find('ItemCode').text if line.find('ItemCode') is not None else ''
                    
                    # Skip if no barcode
                    if not item_code or len(item_code) < 8:
                        continue
                    
                    product = TransparencyProduct(
                        item_code=item_code,
                        item_name=line.find('ItemName').text if line.find('ItemName') is not None else '',
                        manufacturer_name=line.find('ManufacturerName').text if line.find('ManufacturerName') is not None else '',
                        manufacturer_description=line.find('ManufacturerItemDescription').text if line.find('ManufacturerItemDescription') is not None else '',
                        price=float(line.find('ItemPrice').text) if line.find('ItemPrice') is not None and line.find('ItemPrice').text else 0.0,
                        quantity=float(line.find('Quantity').text) if line.find('Quantity') is not None and line.find('Quantity').text else 0.0,
                        unit_qty=line.find('UnitQty').text if line.find('UnitQty') is not None else '',
                        unit_of_measure=line.find('UnitOfMeasure').text if line.find('UnitOfMeasure') is not None else '',
                        price_update_date=line.find('PriceUpdateDate').text if line.find('PriceUpdateDate') is not None else '',
                        store_id=store_id,
                        chain_id=chain_id
                    )
                    products.append(product)
                    
                except Exception as e:
                    logger.warning(f"Error parsing line: {e}")
                    continue
            
            logger.info(f"Parsed {len(products)} products from XML")
            
        except Exception as e:
            logger.error(f"Error parsing XML file: {e}")
        
        return products
    
    def update_database_with_barcodes(self, products: List[TransparencyProduct]):
        """Update existing products with barcodes or insert new ones"""
        cursor = self.conn.cursor()
        
        updated = 0
        inserted = 0
        
        for product in products:
            # First, try to find existing product by name and brand
            cursor.execute("""
                SELECT product_id, attributes
                FROM products
                WHERE canonical_name = %s
                AND (brand = %s OR brand IS NULL)
                LIMIT 1
            """, (product.item_name, product.manufacturer_name))
            
            result = cursor.fetchone()
            
            if result:
                # Update existing product with barcode
                product_id, attributes = result
                if attributes is None:
                    attributes = {}
                
                # Add barcode to attributes
                attributes['barcode'] = product.item_code
                attributes['ean'] = product.item_code
                attributes['manufacturer_description'] = product.manufacturer_description
                
                cursor.execute("""
                    UPDATE products
                    SET attributes = %s
                    WHERE product_id = %s
                """, (json.dumps(attributes), product_id))
                
                updated += 1
            else:
                # Insert new product with barcode
                attributes = {
                    'barcode': product.item_code,
                    'ean': product.item_code,
                    'manufacturer_description': product.manufacturer_description,
                    'unit_qty': product.unit_qty,
                    'unit_of_measure': product.unit_of_measure
                }
                
                cursor.execute("""
                    INSERT INTO products (canonical_name, brand, attributes, description)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (lower(canonical_name), lower(brand)) DO UPDATE
                    SET attributes = EXCLUDED.attributes
                    RETURNING product_id
                """, (
                    product.item_name,
                    product.manufacturer_name,
                    json.dumps(attributes),
                    product.manufacturer_description
                ))
                
                inserted += 1
        
        self.conn.commit()
        cursor.close()
        
        logger.info(f"Updated {updated} products with barcodes")
        logger.info(f"Inserted {inserted} new products with barcodes")
        
        return updated, inserted
    
    def analyze_barcode_coverage(self):
        """Analyze how many products now have barcodes"""
        cursor = self.conn.cursor()
        
        # Count products with barcodes
        cursor.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(CASE WHEN attributes::text LIKE '%barcode%' THEN 1 END) as with_barcode,
                COUNT(CASE WHEN attributes::text LIKE '%ean%' THEN 1 END) as with_ean
            FROM products
        """)
        
        total, with_barcode, with_ean = cursor.fetchone()
        
        logger.info(f"\nBARCODE COVERAGE ANALYSIS:")
        logger.info(f"Total products: {total}")
        logger.info(f"Products with barcode: {with_barcode} ({with_barcode/total*100:.1f}%)")
        logger.info(f"Products with EAN: {with_ean} ({with_ean/total*100:.1f}%)")
        
        # Show sample products with barcodes
        cursor.execute("""
            SELECT 
                canonical_name,
                brand,
                attributes->>'barcode' as barcode,
                attributes->>'ean' as ean
            FROM products
            WHERE attributes::text LIKE '%barcode%'
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        if samples:
            logger.info("\nSample products with barcodes:")
            for name, brand, barcode, ean in samples:
                logger.info(f"  {name} ({brand}): {barcode or ean}")
        
        cursor.close()
    
    def test_barcode_matching(self):
        """Test how many commercial products can now match via barcode"""
        # Load commercial products
        commercial_barcodes = {}
        
        with open('/Users/noa/Desktop/PriceComparisonApp/04_utilities/superpharm_products_final.jsonl', 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    if data.get('ean'):
                        commercial_barcodes[data['ean']] = data
                except:
                    continue
        
        cursor = self.conn.cursor()
        
        # Check government products with matching barcodes
        matches = 0
        cursor.execute("""
            SELECT 
                attributes->>'barcode' as barcode,
                canonical_name,
                brand
            FROM products
            WHERE attributes::text LIKE '%barcode%'
        """)
        
        for barcode, name, brand in cursor.fetchall():
            if barcode and barcode in commercial_barcodes:
                matches += 1
                logger.info(f"MATCH: {name} -> {commercial_barcodes[barcode]['name']}")
        
        logger.info(f"\nPotential barcode matches: {matches}")
        cursor.close()

if __name__ == "__main__":
    parser = TransparencyXMLParser()
    
    # Parse the XML file
    xml_file = '/Users/noa/Downloads/PriceFull7290172900007-006-202507200706 2'
    products = parser.parse_xml_file(xml_file)
    
    if products:
        # Update database with barcodes
        updated, inserted = parser.update_database_with_barcodes(products)
        
        # Analyze coverage
        parser.analyze_barcode_coverage()
        
        # Test matching potential
        parser.test_barcode_matching()