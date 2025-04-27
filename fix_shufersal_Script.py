#!/usr/bin/env python3
"""
Modify the fixed Shufersal script to improve product extraction
"""

import re

# Read the fixed script
with open("fixed_shufersal_script.py", "r", encoding="utf-8") as f:
    content = f.read()

# Find the start and end of the extract_product_metadata function
start_marker = "def extract_product_metadata"
start_pos = content.find(start_marker)

if start_pos == -1:
    print("Could not find extract_product_metadata function!")
    exit(1)

# Find the next def after the function
next_def_pos = content.find("\ndef ", start_pos + 1)
if next_def_pos == -1:
    next_def_pos = len(content)  # If no next def, use end of file

# Construct the improved function with proper indentation
improved_function = """def extract_product_metadata(product_data):
    \"\"\"Extract structured metadata with correct handling of percentages\"\"\"
    product_id = product_data.get("code", "")
    full_name = product_data.get("name", "")
    
    # Initialize metadata
    metadata = {
        "product_id": f"shufersal_{product_id}",
        "name": "",
        "name_he": "",
        "brand": product_data.get("brandName", ""),
        "price": float(product_data.get("price", {}).get("value", 0) or 0),
        "amount": None,  # Use None for MongoDB (will be omitted if null)
        "unit": None,
        "retailer": "Shufersal",
        "source": "shufersal",
        "source_id": product_id
    }
    
    # Early exit for empty name
    if not full_name:
        return metadata
    
    # STEP 1: Separate Hebrew and non-Hebrew parts
    hebrew_parts = []
    non_hebrew_parts = []
    
    for word in full_name.split():
        if any(c in HEBREW_CHARS for c in word):
            hebrew_parts.append(word)
        elif word.replace('%', '').replace('.', '').isdigit():
            # Keep percentages and numbers with non-Hebrew
            non_hebrew_parts.append(word)
        else:
            non_hebrew_parts.append(word)
    
    # STEP 2: Check for percentage patterns
    percentage_match = re.search(r'(\\d+(?:\\.\\d+)?)%', full_name)
    is_dairy = any(term in ' '.join(hebrew_parts) for term in 
                  ["חלב", "יוגורט", "גבינה", "שמנת", "לבן"])
    
    if percentage_match and is_dairy:
        # DAIRY PRODUCT WITH FAT PERCENTAGE
        fat_percentage = percentage_match.group(1)
        
        # Clean Hebrew name (remove fat references)
        clean_hebrew = re.sub(r'\\s*\\d+(?:\\.\\d+)?%\\s*שומן?\\s*', ' ', ' '.join(hebrew_parts))
        clean_hebrew = re.sub(r'\\s+', ' ', clean_hebrew).strip()
        
        # Set appropriate English name based on product type
        if "חלב" in clean_hebrew:
            if "דל לקטוז" in clean_hebrew:
                name_en = "Lactose-Free Milk"
            else:
                name_en = "Milk"
        elif "יוגורט" in clean_hebrew:
            name_en = "Yogurt"
        elif "גבינה" in clean_hebrew:
            if "לבנה" in clean_hebrew:
                name_en = "White Cheese"
            else:
                name_en = "Cheese"
        elif "שמנת" in clean_hebrew:
            name_en = "Cream"
        elif "לבן" in clean_hebrew:
            name_en = "Sour Milk"
        elif "קוטג" in clean_hebrew:
            name_en = "Cottage Cheese"
        else:
            name_en = "Dairy Product"
        
        # Set metadata
        metadata["name"] = name_en
        metadata["name_he"] = clean_hebrew
        metadata["amount"] = float(fat_percentage) if fat_percentage.replace('.', '').isdigit() else fat_percentage
        metadata["unit"] = "%"
    else:
        # REGULAR PRODUCT
        # Set names
        metadata["name_he"] = ' '.join(hebrew_parts) if hebrew_parts else None
        metadata["name"] = ' '.join(non_hebrew_parts) if non_hebrew_parts else full_name
        
        # Try to extract amount and unit
        amount_match = re.search(r'(\\d+(?:\\.\\d+)?)\\s*(גרם|ג\\'|ג|מ"ל|מל|ק"ג|קג|ליטר|ל|יח\\'|ml|g|kg|l|liter)', full_name)
        
        if amount_match:
            amount = amount_match.group(1)
            unit = amount_match.group(2)
            
            # Standardize units
            unit_mapping = {
                'גרם': 'g', 'ג\\'': 'g', 'ג': 'g',
                'מ"ל': 'ml', 'מל': 'ml',
                'ק"ג': 'kg', 'קג': 'kg',
                'ליטר': 'l', 'ל': 'l',
                'יח\\'': 'unit',
                '%': '%'
            }
            
            metadata["amount"] = float(amount) if amount.replace('.', '').isdigit() else amount
            metadata["unit"] = unit_mapping.get(unit, unit)
    
    # Make sure empty values are properly represented for MongoDB
    for key, value in metadata.items():
        if value == "":
            metadata[key] = None
    
    return metadata"""

# Replace the function in the content
new_content = content[:start_pos] + improved_function + content[next_def_pos:]

# Fix escape sequences for regex patterns
new_content = new_content.replace('\\\\d', '\\d')
new_content = new_content.replace('\\\\.', '\\.')
new_content = new_content.replace('\\\\s', '\\s')

# Write the improved script
with open("better_shufersal_script.py", "w", encoding="utf-8") as f:
    f.write(new_content)

print("Created better_shufersal_script.py with improved product extraction logic")