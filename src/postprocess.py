#!/usr/bin/env python3
# src/postprocess.py - Post-processing for OCR results

import re
from typing import List, Tuple, Optional

def clean_ocr_text(lines: List[str]) -> List[str]:
    """
    Clean garbled OCR lines: fix common OCR errors and remove garbage lines.
    
    Args:
        lines (list): Raw OCR text lines
        
    Returns:
        list: Cleaned text lines
    """
    cleaned_lines = []
    for line in lines:
        line = line.replace("₪", "₪")
        line = line.replace("S", "₪") if "₪" not in line and re.search(r'\d', line) else line
        line = line.replace("O", "0").replace("I", "1").replace("l", "1")
        line = re.sub(r"[^א-תA-Za-z0-9₪.,:/\s%-]", "", line)

        if len(line.strip()) > 2 and re.search(r'[א-תA-Za-z0-9]', line):
            cleaned_lines.append(line.strip())
    return cleaned_lines

# In src/postprocess.py, update extract_product_and_price function

def extract_product_and_price(lines: List[str]) -> List[Tuple[str, Optional[float]]]:
    """
    Extract product name and price from cleaned OCR lines.
    Looks for price patterns and merges up to two meaningful lines above it.
    """
    results = []
    price_patterns = [
        r'₪\s*([\d.,]+)',            # Standard shekel price
        r'w\s*([\d.,]+)',            # 'w' is often detected instead of ₪
        r'([\d.,]+)\s*₪',            # Price followed by shekel
        r'שח\s*(?:ל-|7-)\s*100',      # Price per 100g in Hebrew
        r'(\d+[.,]\d+)(?:\s*שח|₪)?',  # Number that looks like a price
        r'(?:₪|w)?\s*(\d+[.,]\d+)'   # Price with or without a symbol
    ]

    for i, line in enumerate(lines):
        price = None
        for pat in price_patterns:
            match = re.search(pat, line)
            if match:
                # Ensure the pattern captured group 1 (the price string)
                if match.lastindex is None or match.lastindex < 1:
                    # This pattern matched but didn't capture a price value (e.g., 'per 100g')
                    continue # Skip to the next pattern

                price_str = match.group(1).replace(',', '.')
                # Also update the price extraction logic
                try:
                    price_val = float(price_str)
                    # Only accept prices in reasonable range for Nutella (5-70 NIS)
                    if 5.0 <= price_val <= 70.0:
                        price = price_val # Assign valid price
                        break # Found a valid price for this line, stop checking patterns
                    else:
                        # Price out of range, reset price and continue check other patterns
                        price = None 
                except ValueError:
                    # Cannot convert to float, reset price and continue check other patterns
                    price = None

        if price is not None:
            title_lines = []
            # Look for product names in lines above the price
            for j in range(i - 1, max(0, i - 5), -1):
                if re.search(r'[א-תA-Za-z0-9]', lines[j]) or "nutella" in lines[j].lower():
                    title_lines.insert(0, lines[j])
                    if len(title_lines) == 2:
                        break

            product_name = " ".join(title_lines) if title_lines else "UNKNOWN"
            product_name = product_name.replace("|", "").replace("  ", " ").strip()
            
            # Check again if the product name has Nutella-related keywords
            if "נוטלה" in product_name.lower() or "nutella" in product_name.lower() or "ממרח" in product_name.lower():
                results.append((product_name, price))

    return results