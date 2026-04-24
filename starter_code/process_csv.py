import csv
import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.


def _parse_price(value):
    if not value or value.strip() == '':
        return None
    s = str(value).strip().lower()
    if s in ('n/a', 'null', '', 'liên hệ', 'contact'):
        return None
    if s.startswith('$'):
        try:
            return float(s.replace('$', '').replace(',', ''))
        except ValueError:
            return None
    if s.endswith('vnd') or s.endswith('usd'):
        s = s.replace('vnd', '').replace('usd', '').strip()
    if s.replace('.', '').replace('-', '').isdigit():
        try:
            return float(s)
        except ValueError:
            return None
    word_to_num = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        'a': 1, 'an': 1,
    }
    s_clean = s.replace('dollars', '').replace('dollar', '').strip()
    words = re.split(r'[\s,]+', s_clean)
    total = 0
    for w in words:
        w = re.sub(r'[^a-z0-9]', '', w)
        if w in word_to_num:
            total = total * 10 + word_to_num[w]
    if total > 0:
        return float(total)
    return None


def _parse_date(value):
    if not value or value.strip() == '':
        return None
    s = str(value).strip()
    # Remove ordinal suffix like "16th", "22nd"
    s = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s)
    formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y/%m/%d',
        '%d %b %Y',
        '%B %d, %Y',
        '%d %B %Y',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def process_sales_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Remove duplicates based on 'id', keep first occurrence
    seen_ids = set()
    unique_rows = []
    for row in rows:
        rid = row.get('id', '').strip()
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            unique_rows.append(row)

    results = []
    for row in unique_rows:
        rid = row.get('id', '').strip()
        price_val = _parse_price(row.get('price', ''))
        date_val = _parse_date(row.get('date_of_sale', ''))
        stock_raw = row.get('stock_quantity', '').strip()
        try:
            stock_qty = int(stock_raw) if stock_raw != '' else None
        except ValueError:
            stock_qty = None

        # Skip rows with negative prices or negative stock
        if price_val is not None and price_val < 0:
            continue
        if stock_qty is not None and stock_qty < 0:
            continue

        content_parts = [
            f"Product: {row.get('product_name', '')}",
            f"Category: {row.get('category', '')}",
            f"Price: {price_val}",
            f"Currency: {row.get('currency', '')}",
            f"Date: {date_val}",
            f"Seller: {row.get('seller_id', '')}",
            f"Stock: {stock_qty}",
        ]
        content = ' | '.join(p for p in content_parts if p)
        original_id = int(float(rid)) if rid else None

        doc = UnifiedDocument(
            document_id=f"csv-{original_id}" if original_id is not None else "csv-unknown",
            content=content,
            source_type='CSV',
            author='Sales System',
            timestamp=datetime.now(),
            source_metadata={
                'original_id': original_id,
                'product_name': row.get('product_name', ''),
                'category': row.get('category', ''),
                'price': price_val,
                'currency': row.get('currency', ''),
                'date_of_sale': date_val,
                'seller_id': row.get('seller_id', ''),
                'stock_quantity': stock_qty,
            }
        )
        results.append(doc)
    return results
