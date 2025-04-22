import re
from typing import List, Tuple, Optional

def clean_ocr_text(lines: List[str]) -> List[str]:
    """
    Clean garbled OCR lines: fix common OCR errors and remove garbage lines.
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

def extract_product_and_price(lines: List[str]) -> List[Tuple[str, Optional[float]]]:
    """
    Extract product name and price from cleaned OCR lines.
    Looks for price patterns and merges up to two meaningful lines above it.
    """
    results = []
    price_patterns = [
        r'₪\s*[:/]?\s*([\d.,]+)',
        r'([\d]{1,3}(?:[.,][\d]{1,2}))'
    ]

    for i, line in enumerate(lines):
        price = None
        for pat in price_patterns:
            match = re.search(pat, line)
            if match:
                price_str = match.group(1).replace(',', '.')
                try:
                    price = float(price_str)
                except ValueError:
                    price = None
                break

        if price is not None:
            title_lines = []
            for j in range(i - 1, -1, -1):
                if re.search(r'[א-ת]', lines[j]):
                    title_lines.insert(0, lines[j])
                    if len(title_lines) == 2:
                        break

            product_name = " ".join(title_lines) if title_lines else "UNKNOWN"
            product_name = product_name.replace("|", "").replace("  ", " ").strip()
            results.append((product_name, price))

    return results
import re
from typing import List, Tuple, Optional

def clean_ocr_text(lines: List[str]) -> List[str]:
    """
    Clean garbled OCR lines: fix common OCR errors and remove garbage lines.
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

def extract_product_and_price(lines: List[str]) -> List[Tuple[str, Optional[float]]]:
    """
    Extract product name and price from cleaned OCR lines.
    Looks for price patterns and merges up to two meaningful lines above it.
    """
    results = []
    price_patterns = [
        r'₪\s*[:/]?\s*([\d.,]+)',
        r'([\d]{1,3}(?:[.,][\d]{1,2}))'
    ]

    for i, line in enumerate(lines):
        price = None
        for pat in price_patterns:
            match = re.search(pat, line)
            if match:
                price_str = match.group(1).replace(',', '.')
                try:
                    price = float(price_str)
                except ValueError:
                    price = None
                break

        if price is not None:
            title_lines = []
            for j in range(i - 1, -1, -1):
                if re.search(r'[א-ת]', lines[j]):
                    title_lines.insert(0, lines[j])
                    if len(title_lines) == 2:
                        break

            product_name = " ".join(title_lines) if title_lines else "UNKNOWN"
            product_name = product_name.replace("|", "").replace("  ", " ").strip()
            results.append((product_name, price))

    return results
