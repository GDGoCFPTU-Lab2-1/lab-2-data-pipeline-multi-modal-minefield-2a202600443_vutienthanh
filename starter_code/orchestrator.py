import json
import time
import os

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")


# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.


def _ingest(doc_or_list, final_kb, source_name):
    """Helper: handle single doc or list of docs."""
    items = doc_or_list if isinstance(doc_or_list, list) else [doc_or_list]
    count = 0
    for item in items:
        if item is None:
            continue
        doc_dict = item if isinstance(item, dict) else item.model_dump()
        if run_quality_gate(doc_dict):
            final_kb.append(doc_dict)
            count += 1
        else:
            print(f"  [SKIP] {doc_dict.get('document_id', 'unknown')} rejected by quality gate")
    print(f"  -> {count} document(s) from {source_name} passed quality gate")
    return count


def main():
    start_time = time.time()
    final_kb = []

    # --- FILE PATH SETUP (Handled for students) ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ----------------------------------------------

    # --- PDF ---
    print("Processing PDF...")
    try:
        pdf_doc = extract_pdf_data(pdf_path)
        _ingest(pdf_doc, final_kb, "PDF")
    except Exception as e:
        print(f"  [ERROR] PDF processing failed: {e}")

    # --- Transcript ---
    print("Processing Transcript...")
    try:
        trans_doc = clean_transcript(trans_path)
        _ingest(trans_doc, final_kb, "Transcript")
    except Exception as e:
        print(f"  [ERROR] Transcript processing failed: {e}")

    # --- HTML Catalog ---
    print("Processing HTML Catalog...")
    try:
        html_docs = parse_html_catalog(html_path)
        _ingest(html_docs, final_kb, "HTML")
    except Exception as e:
        print(f"  [ERROR] HTML processing failed: {e}")

    # --- CSV Sales Records ---
    print("Processing CSV Sales Records...")
    try:
        csv_docs = process_sales_csv(csv_path)
        _ingest(csv_docs, final_kb, "CSV")
    except Exception as e:
        print(f"  [ERROR] CSV processing failed: {e}")

    # --- Legacy Code ---
    print("Processing Legacy Code...")
    try:
        code_doc = extract_logic_from_code(code_path)
        _ingest(code_doc, final_kb, "Code")
    except Exception as e:
        print(f"  [ERROR] Legacy code processing failed: {e}")

    # --- Save final KB ---
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_kb, f, ensure_ascii=False, indent=2, default=str)

    end_time = time.time()
    print(f"\nPipeline finished in {end_time - start_time:.2f} seconds.")
    print(f"Total valid documents stored: {len(final_kb)}")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
