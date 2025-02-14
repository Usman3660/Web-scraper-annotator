import os
import mysql.connector
import time
from PyPDF2 import PdfReader
from google import genai

# Gemini API configuration using only the API key
API_KEY = "absndfJnNaKNSNDSSKDMSKD"  # Replace with your actual API key
client = genai.Client(api_key=API_KEY)
CATEGORIES = ["Deep Learning", "Computer Vision", "Reinforcement Learning", "NLP", "Optimization"]

def get_db_connection():
    """Establish and return a connection to the MySQL database."""
    return mysql.connector.connect(
        host="localhost",
        user="root",        # Adjust if necessary
        password="",        # Set password if applicable
        database="dsscrapping"  # Your database name
    )

def get_unlabeled_papers():
    """
    Fetch papers from the database where 'label' is NULL or 'Uncategorized'.
    Returns a list of dictionaries with paper id and file_path.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    query = """
        SELECT id, file_path 
        FROM papers
        WHERE label IS NULL OR label = 'Uncategorized'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    return rows

def update_label(paper_id, new_label):
    """Update the 'label' for a given paper id in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "UPDATE papers SET label = %s WHERE id = %s"
    cursor.execute(query, (new_label, paper_id))
    conn.commit()
    conn.close()

def extract_text(file_path):
    """
    Extract text content from a file.
    - For TXT files, read its contents.
    - For PDF files, extract text from all pages using PyPDF2.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return ""
    
    if file_path.lower().endswith(".txt"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            print(f"Error reading text file {file_path}: {e}")
            return ""
    elif file_path.lower().endswith(".pdf"):
        try:
            reader = PdfReader(file_path)
            text = ""
            for page in reader.pages:
                page_text = page.extract_text() or ""
                text += page_text
            return text
        except Exception as e:
            print(f"Error reading PDF {file_path}: {e}")
            return ""
    else:
        print(f"Unsupported file format for {file_path}")
        return ""

def annotate_with_gemini(text, retries=3):
    """
    Send the full text of the paper to the Gemini API using the genai client.
    The prompt instructs the API to classify the paper into one of the predefined categories.
    Returns the validated category or "Uncategorized" if no match is found.
    """
    prompt = (
        "Classify this research paper into one of these categories: "
        "Deep Learning, Computer Vision, Reinforcement Learning, NLP, Optimization. "
        "Return only one of these exact words, without any additional text.\n\n"
        f"Paper Text: {text}\nCategory:"
    )
    
    for _ in range(retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt
            )
            category = response.text.strip()
            print(f"Debug: API returned: '{category}'")
            # Validate the returned category against the expected list
            for cat in CATEGORIES:
                if cat.lower() in category.lower():
                    return cat
            return "Uncategorized"
        except Exception as e:
            print(f"⚠ API Error: {e}")
            time.sleep(2)
    return None

def main():
    papers = get_unlabeled_papers()
    if not papers:
        print("No unlabeled papers found.")
        return

    for paper in papers:
        paper_id = paper["id"]
        file_path = paper["file_path"]
        print(f"\nAnnotating paper ID {paper_id} from file: {file_path}")
        
        # Extract the full text from the paper file.
        paper_text = extract_text(file_path)
        if not paper_text.strip():
            print("No text extracted from the paper. Skipping.")
            continue
        
        # Annotate the paper by sending its full text to the Gemini API.
        category = annotate_with_gemini(paper_text)
        if category:
            print(f"Paper ID {paper_id} classified as: {category}")
            update_label(paper_id, category)
        else:
            print(f"Failed to annotate paper ID {paper_id}.")

if __name__ == "__main__":
    main()
