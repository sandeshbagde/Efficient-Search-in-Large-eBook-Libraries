import whoosh
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, abort
import os
import logging
from whoosh.fields import Schema, TEXT, ID, NUMERIC, DATETIME
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from whoosh import index
from whoosh import writing
from PyPDF2 import PdfReader
import re
from datetime import datetime
import mimetypes
import shutil
import sys

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'dev_key'
mimetypes.init()  # Initialize mimetypes for file downloads

# Project paths
BOOKS_DIR = 'books'
INDEX_DIR = 'indexdir'

# Define the current schema
current_schema = Schema(
    title=TEXT(stored=True),
    content=TEXT(stored=True),
    path=ID(stored=True, unique=True),
    page_count=NUMERIC(stored=True),
    category=TEXT(stored=True),
    date_added=DATETIME(stored=True)
)

def extract_text_from_pdf(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = " ".join(page.extract_text() or "" for page in reader.pages)
        text = re.sub(r'\s+', ' ', text).strip()
        if not text:
            logger.warning(f"No text extracted from {pdf_path}. Possibly image-based PDF.")
        return text
    except Exception as e:
        logger.error(f"PyPDF2 extraction failed for {pdf_path}: {str(e)}")
        return ""

def create_or_open_index():
    if not os.path.exists(INDEX_DIR):
        os.mkdir(INDEX_DIR)
        logger.info(f"Created new index directory: {INDEX_DIR}")
        return create_in(INDEX_DIR, current_schema)
    
    try:
        ix = open_dir(INDEX_DIR)
        existing_schema = ix.schema
        if not all(field in existing_schema.names() for field in current_schema.names()):
            logger.warning(f"Schema mismatch detected. Rebuilding index from {INDEX_DIR}")
            shutil.rmtree(INDEX_DIR, ignore_errors=True)
            os.mkdir(INDEX_DIR)
            return create_in(INDEX_DIR, current_schema)
        return ix
    except Exception as e:
        logger.error(f"Failed to open index directory {INDEX_DIR}: {str(e)}. Rebuilding index.")
        shutil.rmtree(INDEX_DIR, ignore_errors=True)
        os.mkdir(INDEX_DIR)
        return create_in(INDEX_DIR, current_schema)

def index_books(ix):
    try:
        with ix.writer() as writer:
            indexed_files = set()
            for filename in os.listdir(BOOKS_DIR):
                if filename.lower().endswith('.pdf') and filename not in indexed_files:
                    pdf_path = os.path.join(BOOKS_DIR, filename)
                    if os.path.exists(pdf_path):
                        title = os.path.splitext(filename)[0]
                        content = extract_text_from_pdf(pdf_path)
                        if content:
                            writer.add_document(
                                title=title,
                                content=content,
                                path=filename,
                                page_count=len(PdfReader(pdf_path).pages),
                                category="Uncategorized",
                                date_added=datetime.now()
                            )
                            indexed_files.add(filename)
            logger.info(f"Indexed {len(indexed_files)} books.")
        return len(indexed_files)
    except Exception as e:
        logger.error(f"Indexing error: {str(e)}")
        return 0

@app.route('/')
@app.route('/home')
def home():
    try:
        if not os.path.exists(BOOKS_DIR):
            os.makedirs(BOOKS_DIR)
        raw_books = [f for f in os.listdir(BOOKS_DIR) if f.lower().endswith('.pdf')]
        books = [{'filename': book, 'title': os.path.splitext(book)[0], 'category': 'Uncategorized', 'date_added': datetime.fromtimestamp(os.path.getctime(os.path.join(BOOKS_DIR, book))).strftime('%Y-%m-%d')} for book in raw_books]
        return render_template('index.html', books=books, page=1, per_page=10)
    except Exception as e:
        logger.error(f"Home route error: {str(e)}")
        flash(f"Error loading home page: {e}", 'danger')
        return redirect(url_for('home'))

@app.route('/upload')
def upload_page():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        logger.info("Upload attempt started.")
        if 'file' not in request.files:
            logger.warning("No file part in request.")
            flash('No file selected.', 'danger')
            return redirect(url_for('upload_page'))
        
        file = request.files['file']
        logger.info(f"Received file: {file.filename}")
        category = request.form.get('category', 'Uncategorized')
        if file.filename == '' or not file.filename.lower().endswith('.pdf'):
            logger.warning(f"Invalid file: {file.filename}")
            flash('Please select a valid PDF file.', 'danger')
            return redirect(url_for('upload_page'))
        
        filepath = os.path.join(BOOKS_DIR, file.filename)
        logger.info(f"Target filepath: {filepath}")
        if not os.path.exists(BOOKS_DIR):
            os.makedirs(BOOKS_DIR)
            logger.info(f"Created directory: {BOOKS_DIR}")
        
        if os.path.exists(filepath):
            logger.warning(f"File already exists: {filepath}")
            flash('File already exists. Please rename and try again.', 'warning')
            return redirect(url_for('upload_page'))
        
        file.save(filepath)
        logger.info(f"File saved successfully: {filepath}")
        ix = create_or_open_index()
        with ix.writer() as writer:
            title = os.path.splitext(file.filename)[0]
            content = extract_text_from_pdf(filepath)
            logger.info(f"Extracted content length: {len(content) if content else 0}")
            if content:
                writer.add_document(
                    title=title,
                    content=content,
                    path=file.filename,
                    page_count=len(PdfReader(filepath).pages),
                    category=category,
                    date_added=datetime.now()
                )
                logger.info(f"Indexed document: {file.filename}")
                flash(f'Uploaded and indexed: {file.filename} (Category: {category})', 'success')
            else:
                logger.warning(f"No text extracted from {file.filename}")
                flash(f'Failed to extract text from {file.filename}. File may be image-based.', 'danger')
                os.remove(filepath)
        return redirect(url_for('upload_page'))
    except PermissionError as e:
        logger.error(f"Permission denied while uploading: {str(e)}")
        flash(f'Permission denied: {e}. Check directory permissions.', 'danger')
        return redirect(url_for('upload_page'))
    except Exception as e:
        logger.error(f"Upload error: {str(e)}", exc_info=True)  # Include full traceback
        flash(f'Error uploading file: {e}', 'danger')
        return redirect(url_for('upload_page'))

@app.route('/search')
def search_page():
    return render_template('search.html')

@app.route('/search', methods=['POST'])
def search():
    try:
        query_str = request.form['query']
        sort_by = request.form.get('sort_by', 'relevance')
        if not query_str.strip():
            flash('Please enter a search term.', 'warning')
            return redirect(url_for('search_page'))
        
        ix = create_or_open_index()
        with ix.searcher() as searcher:
            query = QueryParser("content", ix.schema).parse(query_str)
            results = searcher.search(query, limit=100)
            
            if len(results) == 0:
                logger.warning(f"No results for query '{query_str}'. Triggering re-index.")
                flash('No results found. Re-indexing library...', 'info')
                index_books(ix)
                results = searcher.search(query, limit=100)
            
            search_results = []
            for result in results:
                snippet = result.highlights("content", top=1)
                search_results.append({
                    'title': result['title'],
                    'path': result['path'],
                    'score': result.score,
                    'snippet': snippet or 'No snippet available.',
                    'category': result.get('category', 'Uncategorized'),
                    'date_added': result.get('date_added', datetime.now()).strftime('%Y-%m-%d')
                })
            
            if sort_by == 'title':
                search_results.sort(key=lambda x: x['title'])
            elif sort_by == 'date':
                search_results.sort(key=lambda x: datetime.strptime(x['date_added'], '%Y-%m-%d'), reverse=True)
            
            per_page = 5
            page = int(request.form.get('page', 1))
            start = (page - 1) * per_page
            end = start + per_page
            paginated_results = search_results[start:end]
            total_pages = (len(search_results) + per_page - 1) // per_page

        return render_template('search.html', results=paginated_results, query=query_str, page=page, total_pages=total_pages, sort_by=sort_by)
    except Exception as e:
        logger.error(f"Search error for query '{query_str}': {str(e)}")
        flash(f'Search failed: {e}. Check logs or re-index library.', 'danger')
        return redirect(url_for('search_page'))

@app.route('/delete/<filename>', methods=['POST'])
def delete(filename):
    try:
        filepath = os.path.join(BOOKS_DIR, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Successfully deleted file: {filepath}")
            ix = create_or_open_index()
            with ix.writer() as writer:
                writer.delete_by_term('path', filename)
                flash(f'Deleted: {filename} and updated index.', 'success')
        else:
            flash(f'File not found: {filename}', 'danger')
        return redirect(url_for('home'))
    except PermissionError as e:
        logger.error(f"Permission denied while deleting {filepath}: {str(e)}")
        flash(f'Permission denied: {e}', 'danger')
        return redirect(url_for('home'))
    except Exception as e:
        logger.error(f"Delete error for {filename}: {str(e)}")
        flash(f'Error deleting file: {e}', 'danger')
        return redirect(url_for('home'))

@app.route('/details/<filename>')
def details(filename):
    try:
        filepath = os.path.join(BOOKS_DIR, filename)
        if os.path.exists(filepath):
            reader = PdfReader(filepath)
            page_count = len(reader.pages)
            excerpt = reader.pages[0].extract_text()[:200] + "..." if reader.pages else "No excerpt available."
            return render_template('details.html', filename=filename, title=os.path.splitext(filename)[0], page_count=page_count, excerpt=excerpt)
        abort(404)
    except Exception as e:
        logger.error(f"Details error for {filename}: {str(e)}")
        flash(f'Error loading details: {e}', 'danger')
        return redirect(url_for('home'))

@app.route('/download/<filename>')
def download(filename):
    try:
        filepath = os.path.join(BOOKS_DIR, filename)
        if os.path.exists(filepath):
            return send_from_directory(BOOKS_DIR, filename, as_attachment=True)
        abort(404)
    except Exception as e:
        logger.error(f"Download error for {filename}: {str(e)}")
        flash(f'Error downloading file: {e}', 'danger')
        return redirect(url_for('home'))

@app.route('/index_books')
def index_books_route():
    try:
        ix = create_or_open_index()
        num_indexed = index_books(ix)
        flash(f'Re-indexed {num_indexed} books successfully!', 'info')
    except Exception as e:
        logger.error(f"Re-index error: {str(e)}")
        flash(f'Re-index failed: {e}. Check logs for details.', 'danger')
    return redirect(url_for('home'))

@app.route('/books/<path:filename>')
def serve_pdf(filename):
    try:
        return send_from_directory(BOOKS_DIR, filename)
    except FileNotFoundError:
        abort(404)

if __name__ == '__main__':
    try:
        logger.info("Starting application...")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Current directory: {os.getcwd()}")
        logger.info(f"Checking directories: books={os.path.exists(BOOKS_DIR)}, indexdir={os.path.exists(INDEX_DIR)}")
        
        # Verify template directory and files
        template_dir = os.path.join(os.getcwd(), 'templates')
        if not os.path.exists(template_dir):
            logger.error(f"Template directory not found at {template_dir}")
            raise FileNotFoundError(f"Template directory 'templates' not found. Create it and add index.html, upload.html, search.html, details.html.")
        required_templates = ['index.html', 'upload.html', 'search.html', 'details.html']
        for template in required_templates:
            if not os.path.exists(os.path.join(template_dir, template)):
                logger.error(f"Template {template} not found in {template_dir}")
                raise FileNotFoundError(f"Template {template} missing. Ensure all templates are in 'templates/'.")

        # Verify static directory
        static_dir = os.path.join(os.getcwd(), 'static')
        if not os.path.exists(static_dir) or not os.path.exists(os.path.join(static_dir, 'style.css')):
            logger.error(f"Static directory or style.css not found at {static_dir}")
            raise FileNotFoundError(f"Static directory 'static' or style.css missing. Create it and add style.css.")

        # Additional environment checks
        if sys.version_info < (3, 8):
            logger.error(f"Python version {sys.version} is too old. Requires 3.8+.")
            raise RuntimeError("Python 3.8 or higher required.")

        ix = create_or_open_index()
        if not whoosh.index.exists_in(INDEX_DIR):
            logger.info("No existing index found. Initializing indexing...")
            index_books(ix)
        else:
            logger.info("Existing index found. Ready to serve.")
        
        logger.info("Attempting to start Flask server...")
        app.run(debug=True, port=5001, host='0.0.0.0')  # Allow access from any interface
    except Exception as e:
        logger.error(f"Application startup error: {str(e)}", exc_info=True)
        print(f"Startup failed: {e}")
        with open('app.log', 'r') as log_file:
            print("Last 10 lines of app.log:")
            for line in log_file.readlines()[-10:]:
                print(line.strip())