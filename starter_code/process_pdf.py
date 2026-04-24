import os
from datetime import datetime
from schema import UnifiedDocument

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Use Gemini API to extract structured data from lecture_notes.pdf


def extract_pdf_data(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        try:
            from dotenv import load_dotenv
            load_dotenv()
            api_key = os.environ.get('GEMINI_API_KEY')
        except ImportError:
            pass
        if not api_key:
            print("Warning: GEMINI_API_KEY not found. Skipping PDF processing.")
            return None

    try:
        from google import genai
        client = genai.Client(api_key=api_key)
    except ImportError:
        try:
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-2.0-flash')
            has_old_api = True
        except ImportError:
            print("Error: Neither google.genai nor google.generativeai is available.")
            return None
        has_old_api = True
    else:
        has_old_api = False

    prompt = (
        "Extract the following from this PDF lecture notes document:\n"
        "1. Title of the lecture\n"
        "2. Author(s)\n"
        "3. A concise 3-sentence summary of the content\n"
        "Return your response in plain text with clear labels."
    )

    if has_old_api:
        pdf_file = genai.upload_file(path=file_path)
        response = model.generate_content([pdf_file, prompt])
        response_text = response.text
    else:
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=[{
                'parts': [{'file_data': {'mime_type': 'application/pdf', 'file_uri': file_path}}],
            }, {
                'parts': [{'text': prompt}]
            }]
        )
        response_text = response.text if hasattr(response, 'text') else str(response)

    # Parse the response
    title = ''
    author = ''
    summary = ''

    lines = response_text.split('\n')
    for line in lines:
        if line.lower().startswith('title'):
            title = line.split(':', 1)[-1].strip()
        elif line.lower().startswith('author'):
            author = line.split(':', 1)[-1].strip()
        elif line.lower().startswith('summary') or line.lower().startswith('description'):
            summary = line.split(':', 1)[-1].strip()

    if not title and not author and not summary:
        title = 'Lecture Notes'
        summary = response_text[:500]

    content = f"Title: {title}\nAuthor: {author}\nSummary: {summary}"

    doc = UnifiedDocument(
        document_id='pdf-lecture-notes',
        content=content,
        source_type='PDF',
        author=author or 'Unknown',
        timestamp=datetime.now(),
        source_metadata={
            'title': title,
            'summary': summary,
        }
    )
    return doc
