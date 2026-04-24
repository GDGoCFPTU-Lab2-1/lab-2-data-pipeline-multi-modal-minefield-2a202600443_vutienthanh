# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.


TOXIC_STRINGS = [
    'null pointer exception',
    'nullpointerexception',
    'segmentation fault',
    'stack trace',
    'traceback',
    'core dumped',
    'fatal error',
    'system halt',
    'blue screen',
    'bsod',
]

# ==========================================
# Semantic Gates
# ==========================================

def run_quality_gate(document_dict):
    content = document_dict.get('content', '')

    # Gate 1: Minimum content length
    if len(content.strip()) < 20:
        print(f"[GATE FAIL] Content too short ({len(content)} chars) for doc {document_dict.get('document_id', 'unknown')}")
        return False

    # Gate 2: Toxic / corrupt strings
    content_lower = content.lower()
    for toxic in TOXIC_STRINGS:
        if toxic in content_lower:
            print(f"[GATE FAIL] Toxic string '{toxic}' detected in doc {document_dict.get('document_id', 'unknown')}")
            return False

    # Gate 3: Discrepancy detection (e.g. tax comment vs code)
    metadata = document_dict.get('source_metadata', {})
    if metadata.get('has_discrepancy'):
        print(f"[GATE WARN] Discrepancy flagged for doc {document_dict.get('document_id', 'unknown')} — still passing but logged")

    return True
