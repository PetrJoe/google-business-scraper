import os
import threading
import time
from flask import Flask, render_template, request, jsonify, send_file, Response
import logging
from logging.handlers import RotatingFileHandler
from io import StringIO
import csv
import json
from datetime import datetime
from main import EnhancedBusinessScraper

# Create Flask app
app = Flask(__name__)

# Configure logging to capture all output
log_capture_string = StringIO()
string_handler = logging.StreamHandler(log_capture_string)
string_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
string_handler.setFormatter(formatter)

# Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(string_handler)

# Also log to file
# file_handler = RotatingFileHandler('flask_scraper.log', maxBytes=1000000, backupCount=5)
# file_handler.setFormatter(formatter)
# root_logger.addHandler(file_handler)

# Global variables to manage scraping state
scraping_thread = None
scraping_results = []
is_scraping = False
current_query = ""
scraping_error = None

# Modified version of the main function to work with Flask
def run_scraper_in_thread(query, location, max_results, output_format):
    global is_scraping, scraping_results, scraping_error
    
    try:
        # Create a modified args object
        class Args:
            def __init__(self, query, location, max_results, output_format):
                self.query = query
                self.location = location
                self.max_results = max_results
                self.max_pages = 3
                self.headless = True
                self.proxies = None
                self.output_csv = (output_format == 'csv')
                self.output_json = (output_format == 'json')
                self.output_excel = False
                self.save_db = False
                self.reference_lat = None
                self.reference_lng = None
        
        args = Args(query, location, max_results, output_format)
        
        # Initialize and run scraper
        scraper = EnhancedBusinessScraper(args)
        
        logging.info(f"Starting scraping for: {query}")
        if location:
            logging.info(f"Location: {location}")
        
        results = scraper.scrape_google_maps()
        
        # Retry failed websites if any
        if hasattr(scraper.website_crawler, 'failed_websites') and scraper.website_crawler.failed_websites:
            results = scraper.retry_failed_websites(results)
        
        # Store results
        scraping_results = results
        
        logging.info(f"Scraping completed successfully. Found {len(results)} results.")
        
    except Exception as e:
        logging.error(f"Error in scraping thread: {str(e)}")
        scraping_error = str(e)
    finally:
        is_scraping = False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    global scraping_thread, is_scraping, current_query, scraping_error
    
    if is_scraping:
        return jsonify({'status': 'error', 'message': 'Scraping is already in progress'})
    
    query = request.form.get('query')
    location = request.form.get('location', '')
    max_results = request.form.get('max_results', '20')
    output_format = request.form.get('output_format', 'csv')
    
    if not query:
        return jsonify({'status': 'error', 'message': 'Query is required'})
    
    try:
        max_results = int(max_results)
        if max_results < 1 or max_results > 100:
            return jsonify({'status': 'error', 'message': 'Max results must be between 1 and 100'})
    except ValueError:
        return jsonify({'status': 'error', 'message': 'Max results must be a valid number'})
    
    # Reset error state
    scraping_error = None
    
    current_query = query
    is_scraping = True
    scraping_results = []
    
    # Start scraping in a separate thread
    scraping_thread = threading.Thread(
        target=run_scraper_in_thread,
        args=(query, location, max_results, output_format)
    )
    scraping_thread.daemon = True
    scraping_thread.start()
    
    return jsonify({'status': 'success', 'message': 'Scraping started'})

@app.route('/scraping_status')
def scraping_status():
    global is_scraping, scraping_results, scraping_error
    
    # Get the log contents
    log_contents = log_capture_string.getvalue()
    log_lines = log_contents.split('\n') if log_contents else []
    
    # Return the last 50 lines of logs
    recent_logs = log_lines[-50:] if len(log_lines) > 50 else log_lines
    
    status_data = {
        'is_scraping': is_scraping,
        'logs': recent_logs,
        'result_count': len(scraping_results)
    }
    
    # Add results if available
    if scraping_results:
        from main import asdict
        status_data['results'] = [asdict(result) for result in scraping_results]
    
    # Add error information if there was an error
    if scraping_error and not is_scraping:
        status_data['error'] = scraping_error
    
    return jsonify(status_data)


@app.route('/download_results')
def download_results():
    global scraping_results
    
    if not scraping_results:
        return jsonify({'status': 'error', 'message': 'No results to download'})
    
    format_type = request.args.get('format', 'csv')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format_type == 'csv':
        filename = f"businesses_{timestamp}.csv"
        
        # Create CSV in memory
        output = StringIO()
        
        if scraping_results:
            from main import asdict
            # Get headers from the first result
            headers = list(asdict(scraping_results[0]).keys())
            writer = csv.DictWriter(output, fieldnames=headers)
            
            # Write header
            writer.writeheader()
            
            # Write data
            for result in scraping_results:
                row = asdict(result)
                row['emails'] = ', '.join(row['emails'])
                row['social_media'] = json.dumps(row['social_media'])
                row['coordinates'] = json.dumps(row['coordinates']) if row['coordinates'] else ''
                writer.writerow(row)
        
        output.seek(0)
        
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={filename}"}
        )
    
    elif format_type == 'json':
        filename = f"businesses_{timestamp}.json"
        
        from main import asdict
        data = [asdict(result) for result in scraping_results]
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        
        return Response(
            json_str,
            mimetype="application/json",
            headers={"Content-disposition": f"attachment; filename={filename}"}
        )
    
    return jsonify({'status': 'error', 'message': 'Invalid format'})





if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    app.run(debug=True, host='0.0.0.0', port=5000)