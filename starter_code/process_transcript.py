import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.


def _extract_price_vnd(text):
    vietnamese_numbers = {
        'một': 1, 'hai': 2, 'ba': 3, 'bốn': 4, 'năm': 5,
        'sáu': 6, 'bảy': 7, 'tám': 8, 'chín': 9, 'mười': 10,
    }
    patterns = [
        r'(năm\s*trăm\s*nghìn)',
        r'(\d+)\s*trăm\s*nghìn',
        r'(\d+)\s*,\s*(\d+)\s*trăm\s*nghìn',
    ]
    match = re.search(r'(năm\s*trăm\s*nghìn)', text, re.IGNORECASE)
    if match:
        return 500000
    match = re.search(r'(\d+)\s*trăm\s*nghìn', text, re.IGNORECASE)
    if match:
        return int(match.group(1)) * 100000
    return None


def clean_transcript(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()

    # Remove noise tokens
    text = re.sub(r'\[Music\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[Music starts\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[Music ends\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[inaudible\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[Laughter\]', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\[Speaker \d+\]', '', text)
    text = re.sub(r'\[00:\d{2}:\d{2}\]', '', text)

    # Strip extra whitespace
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned_content = ' '.join(lines)

    # Extract price in VND from Vietnamese words
    detected_price = _extract_price_vnd(text)

    doc = UnifiedDocument(
        document_id='transcript-lecture-01',
        content=cleaned_content,
        source_type='Video',
        author='VinAI Education',
        timestamp=datetime.now(),
        source_metadata={
            'detected_price_vnd': detected_price,
            'title': 'Data Pipeline Engineering Lecture',
        }
    )
    return doc
