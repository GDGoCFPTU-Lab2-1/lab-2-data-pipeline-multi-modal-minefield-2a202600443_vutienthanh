import ast
import re
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.


def extract_logic_from_code(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    tree = ast.parse(source_code)
    functions = []
    business_rules = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            func_name = node.name
            docstring = ast.get_docstring(node)
            func_lines = source_code.splitlines()[node.lineno - 1:node.end_lineno]
            func_source = '\n'.join(func_lines)

            # Extract business rule comments
            for line in func_lines:
                m = re.search(r'#\s*Business Logic Rule\s*(\d+):\s*(.+)', line, re.IGNORECASE)
                if m:
                    business_rules.append({
                        'rule_id': m.group(1),
                        'description': m.group(2).strip(),
                        'function': func_name,
                    })

            # Detect tax rate discrepancy
            tax_discrepancy = None
            if func_name == 'legacy_tax_calc':
                comment_match = re.search(r'(\d+)%', ' '.join(func_lines))
                if comment_match:
                    comment_rate = int(comment_match.group(1))
                    code_match = re.search(r'(0\.\d+)', ' '.join(func_lines))
                    if code_match:
                        code_rate = float(code_match.group(1))
                        code_rate_pct = int(code_rate * 100)
                        if comment_rate != code_rate_pct:
                            tax_discrepancy = {
                                'comment_says': f'{comment_rate}%',
                                'code_actually': f'{code_rate_pct}%',
                                'function': func_name,
                            }

            functions.append({
                'name': func_name,
                'docstring': docstring or '',
                'source': func_source,
                'tax_discrepancy': tax_discrepancy,
            })

    content_parts = []
    for fn in functions:
        part = f"Function: {fn['name']}\nDocstring: {fn['docstring']}"
        if fn['tax_discrepancy']:
            td = fn['tax_discrepancy']
            part += f"\n[WATCHMAN ALERT] Tax discrepancy detected: comment says {td['comment_says']} but code uses {td['code_actually']}!"
        content_parts.append(part)

    content = '\n---\n'.join(content_parts)

    doc = UnifiedDocument(
        document_id='code-legacy-pipeline',
        content=content,
        source_type='Code',
        author='Senior Dev (retired)',
        timestamp=datetime.now(),
        source_metadata={
            'filename': 'legacy_pipeline.py',
            'business_rules': business_rules,
            'functions': [f['name'] for f in functions],
            'has_discrepancy': any(f['tax_discrepancy'] for f in functions),
        }
    )
    return doc
