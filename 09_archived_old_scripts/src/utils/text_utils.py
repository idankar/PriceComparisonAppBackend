#!/usr/bin/env python3
# text_utils.py - Utility functions for text processing

import re
import unicodedata
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def normalize_text(text):
    """Normalize text by removing extra spaces, newlines, etc.
    
    Args:
        text (str): Text to normalize
        
    Returns:
        str: Normalized text
    """
    if not text:
        return ""
        
    # Convert to string if not already
    text = str(text)
    
    # Normalize unicode characters
    text = unicodedata.normalize('NFKC', text)
    
    # Replace multiple spaces and newlines with a single space
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading and trailing whitespace
    text = text.strip()
    
    return text

def extract_amount_and_unit(text):
    """Extract amount and unit from a text string
    
    Args:
        text (str): Text containing amount and unit
        
    Returns:
        tuple: (amount, unit) or (None, None) if not found
    """
    # Normalize the text first
    text = normalize_text(text)
    
    # Try to match patterns like "500 ml", "1.5 kg", "3 x 250g", etc.
    # This regex handles Hebrew and English units
    pattern = r'(\d+(?:\.\d+)?)\s*(?:[xX×]\s*)?(\d+(?:\.\d+)?)?\s*(גרם|ג\'|גר\'|ג|מ"ל|מל|ק"ג|קג|ליטר|לי|ל|יחידות|יח\'|ml|g|kg|l|liter|gram|unit)'
    
    match = re.search(pattern, text)
    if match:
        # Get primary amount
        amount = float(match.group(1))
        
        # Check if there's a secondary amount (like in "3 x 250g")
        if match.group(2):
            amount *= float(match.group(2))
            
        # Get the unit
        unit = match.group(3)
        
        # Normalize units
        unit_map = {
            # Hebrew
            'גרם': 'g', 'ג\'': 'g', 'גר\'': 'g', 'ג': 'g',
            'מ"ל': 'ml', 'מל': 'ml',
            'ק"ג': 'kg', 'קג': 'kg',
            'ליטר': 'l', 'לי': 'l', 'ל': 'l',
            'יחידות': 'unit', 'יח\'': 'unit',
            # English - already normalized
        }
        
        normalized_unit = unit_map.get(unit, unit)
        
        return amount, normalized_unit
    
    return None, None

def extract_price(text):
    """Extract price from a text string
    
    Args:
        text (str): Text containing price
        
    Returns:
        float: Extracted price or None if not found
    """
    # Normalize the text first
    text = normalize_text(text)
    
    # Try to match price patterns (handles both "₪123.45" and "123.45₪")
    pattern = r'(\d+(?:\.\d+)?)(?:\s*₪|₪\s*)'
    
    match = re.search(pattern, text)
    if match:
        return float(match.group(1))
    
    return None

def extract_brand(text, brand_list=None):
    """Extract brand name from text
    
    Args:
        text (str): Text to extract brand from
        brand_list (list, optional): List of known brands to match against
        
    Returns:
        str: Extracted brand or None if not found
    """
    # Normalize the text first
    text = normalize_text(text)
    
    # If we have a list of brands, try to match against them
    if brand_list:
        for brand in brand_list:
            if brand.lower() in text.lower():
                return brand
    
    # Try to extract brand using heuristics (common patterns in product names)
    # For example, brand often comes first in product name
    words = text.split()
    
    # Check first word - often the brand
    if len(words) > 1 and len(words[0]) > 1:
        return words[0]
    
    return None

def is_hebrew(text):
    """Check if text contains Hebrew characters
    
    Args:
        text (str): Text to check
        
    Returns:
        bool: True if text contains Hebrew characters
    """
    # Hebrew Unicode range: 0x0590-0x05FF
    for char in text:
        if '\u0590' <= char <= '\u05FF':
            return True
    return False 