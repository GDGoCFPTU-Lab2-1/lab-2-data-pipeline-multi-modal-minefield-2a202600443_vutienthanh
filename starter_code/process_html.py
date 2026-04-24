from bs4 import BeautifulSoup
import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.


def _parse_html_price(value):
    if not value or value.strip().lower() in ('n/a', 'liên hệ', 'contact', ''):
        return None
    s = str(value).strip()
    s = s.replace('VND', '').replace('vnd', '').replace(',', '').strip()
    try:
        return float(s)
    except ValueError:
        return None


def parse_html_catalog(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    table = soup.find('table', id='main-catalog')
    if not table:
        return []

    rows = table.find('tbody').find_all('tr')
    results = []
    for i, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) < 6:
            continue

        ma_sp = cells[0].get_text(strip=True)
        ten_sp = cells[1].get_text(strip=True)
        danh_muc = cells[2].get_text(strip=True)
        gia_raw = cells[3].get_text(strip=True)
        ton_kho_raw = cells[4].get_text(strip=True)
        danh_gia = cells[5].get_text(strip=True)

        gia = _parse_html_price(gia_raw)
        try:
            ton_kho = int(ton_kho_raw)
        except ValueError:
            ton_kho = None

        if ton_kho is not None and ton_kho < 0:
            ton_kho = None

        content = f"Product: {ten_sp} | Category: {danh_muc} | Price: {gia} VND | Stock: {ton_kho} | Rating: {danh_gia}"

        doc = UnifiedDocument(
            document_id=f"html-{ma_sp}",
            content=content,
            source_type='HTML',
            author='VinShop Catalog',
            timestamp=datetime.now(),
            source_metadata={
                'ma_san_pham': ma_sp,
                'ten_san_pham': ten_sp,
                'danh_muc': danh_muc,
                'gia': gia,
                'ton_kho': ton_kho,
                'danh_gia': danh_gia,
            }
        )
        results.append(doc)
    return results
