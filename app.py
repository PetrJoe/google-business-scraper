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
file_handler = RotatingFileHandler('flask_scraper.log', maxBytes=1000000, backupCount=5)
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

# Global variables to manage scraping state
scraping_thread = None
scraping_results = []
is_scraping = False
current_query = ""

# Import the scraper components (we'll need to modify the original code slightly)
# We'll keep the original scraper code but make some adjustments for Flask integration

# Modified version of the main function to work with Flask
def run_scraper_in_thread(query, location, max_results, output_format):
    global is_scraping, scraping_results
    
    try:
        # Import the scraper components
        import sys
        from io import StringIO
        
        # Capture stdout and stderr
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        captured_output = StringIO()
        sys.stdout = captured_output
        sys.stderr = captured_output
        
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
        
        # Retry failed websites
        if scraper.website_crawler.failed_websites:
            results = scraper.retry_failed_websites(results)
        
        # Store results
        scraping_results = results
        
        # Export results to in-memory files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format == 'csv':
            csv_filename = f"businesses_{timestamp}.csv"
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                if results:
                    from main import asdict
                    writer = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()))
                    writer.writeheader()
                    for result in results:
                        row = asdict(result)
                        row['emails'] = ', '.join(row['emails'])
                        row['social_media'] = json.dumps(row['social_media'])
                        row['coordinates'] = json.dumps(row['coordinates']) if row['coordinates'] else ''
                        writer.writerow(row)
        
        elif output_format == 'json':
            json_filename = f"businesses_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                from main import asdict
                json.dump([asdict(result) for result in results], f, indent=2, ensure_ascii=False)
        
        # Restore stdout and stderr
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        
        # Add the captured output to our logs
        log_contents = captured_output.getvalue()
        if log_contents:
            for line in log_contents.split('\n'):
                if line.strip():
                    logging.info(f"SCRAPER: {line}")
        
        logging.info("Scraping completed successfully")
        
    except Exception as e:
        logging.error(f"Error in scraping thread: {e}")
    finally:
        is_scraping = False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_scraping', methods=['POST'])
def start_scraping():
    global scraping_thread, is_scraping, current_query
    
    if is_scraping:
        return jsonify({'status': 'error', 'message': 'Scraping is already in progress'})
    
    query = request.form.get('query')
    location = request.form.get('location', '')
    max_results = int(request.form.get('max_results', 20))
    output_format = request.form.get('output_format', 'csv')
    
    if not query:
        return jsonify({'status': 'error', 'message': 'Query is required'})
    
    current_query = query
    is_scraping = True
    
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
    global is_scraping, scraping_results
    
    # Get the log contents
    log_contents = log_capture_string.getvalue()
    log_lines = log_contents.split('\n') if log_contents else []
    
    # Return the last 50 lines of logs
    recent_logs = log_lines[-50:] if len(log_lines) > 50 else log_lines
    
    return jsonify({
        'is_scraping': is_scraping,
        'logs': recent_logs,
        'result_count': len(scraping_results)
    })

@app.route('/download_results')
def download_results():
    global scraping_results
    
    format_type = request.args.get('format', 'csv')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format_type == 'csv':
        filename = f"businesses_{timestamp}.csv"
        
        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        
        if scraping_results:
            from main import asdict
            # Write header
            headers = list(asdict(scraping_results[0]).keys())
            writer.writerow(headers)
            
            # Write data
            for result in scraping_results:
                row = asdict(result)
                row['emails'] = ', '.join(row['emails'])
                row['social_media'] = json.dumps(row['social_media'])
                row['coordinates'] = json.dumps(row['coordinates']) if row['coordinates'] else ''
                writer.writerow([str(row.get(h, '')) for h in headers])
        
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
    
    # Create the HTML template
    with open('templates/index.html', 'w') as f:
        f.write('''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Google Maps Business Scraper</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input, select {
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 10px 15px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        button:disabled {
            background-color: #cccccc;
        }
        .logs {
            background-color: #2d2d2d;
            color: #f0f0f0;
            padding: 15px;
            border-radius: 4px;
            height: 300px;
            overflow-y: auto;
            font-family: monospace;
            white-space: pre-wrap;
            margin-top: 20px;
        }
        .status {
            margin: 15px 0;
            padding: 10px;
            border-radius: 4px;
        }
        .status.scraping {
            background-color: #e8f5e9;
            color: #2e7d32;
        }
        .status.idle {
            background-color: #f5f5f5;
            color: #757575;
        }
        .download-section {
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Google Maps Business Scraper</h1>
        
        <form id="scraperForm">
            <div class="form-group">
                <label for="query">Search Query:</label>
                <input type="text" id="query" name="query" required placeholder="e.g., restaurants, hotels, etc.">
            </div>
            
            <div class="form-group">
                <label for="location">Location (optional):</label>
                <input type="text" id="location" name="location" placeholder="e.g., New York, London">
            </div>
            
            <div class="form-group">
                <label for="max_results">Max Results:</label>
                <input type="number" id="max_results" name="max_results" value="20" min="1" max="100">
            </div>
            
            <div class="form-group">
                <label for="output_format">Output Format:</label>
                <select id="output_format" name="output_format">
                    <option value="csv">CSV</option>
                    <option value="json">JSON</option>
                </select>
            </div>
            
            <button type="submit" id="startBtn">Start Scraping</button>
        </form>
        
        <div id="status" class="status idle">
            <span id="statusText">Ready to start scraping</span>
            <span id="resultCount"></span>
        </div>
        
        <div class="download-section" id="downloadSection" style="display: none;">
            <h3>Download Results</h3>
            <button id="downloadCsv">Download CSV</button>
            <button id="downloadJson">Download JSON</button>
        </div>
        
        <h3>Logs:</h3>
        <div class="logs" id="logOutput"></div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const form = document.getElementById('scraperForm');
            const startBtn = document.getElementById('startBtn');
            const statusDiv = document.getElementById('status');
            const statusText = document.getElementById('statusText');
            const resultCount = document.getElementById('resultCount');
            const logOutput = document.getElementById('logOutput');
            const downloadSection = document.getElementById('downloadSection');
            const downloadCsvBtn = document.getElementById('downloadCsv');
            const downloadJsonBtn = document.getElementById('downloadJson');
            
            let pollingInterval;
            
            form.addEventListener('submit', function(e) {
                e.preventDefault();
                startScraping();
            });
            
            downloadCsvBtn.addEventListener('click', function() {
                window.location.href = '/download_results?format=csv';
            });
            
            downloadJsonBtn.addEventListener('click', function() {
                window.location.href = '/download_results?format=json';
            });
            
            function startScraping() {
                const formData = new FormData(form);
                
                startBtn.disabled = true;
                statusDiv.className = 'status scraping';
                statusText.textContent = 'Scraping in progress...';
                logOutput.textContent = 'Starting...\n';
                
                fetch('/start_scraping', {
                    method: 'POST',
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        startPolling();
                    } else {
                        alert('Error: ' + data.message);
                        startBtn.disabled = false;
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    startBtn.disabled = false;
                });
            }
            
            function startPolling() {
                if (pollingInterval) clearInterval(pollingInterval);
                
                pollingInterval = setInterval(() => {
                    fetch('/scraping_status')
                    .then(response => response.json())
                    .then(data => {
                        // Update logs
                        if (data.logs && data.logs.length > 0) {
                            logOutput.textContent = data.logs.join('\n');
                            logOutput.scrollTop = logOutput.scrollHeight;
                        }
                        
                        // Update status
                        if (!data.is_scraping) {
                            clearInterval(pollingInterval);
                            startBtn.disabled = false;
                            statusDiv.className = 'status idle';
                            statusText.textContent = 'Scraping completed';
                            
                            if (data.result_count > 0) {
                                resultCount.textContent = ` - Found ${data.result_count} results`;
                                downloadSection.style.display = 'block';
                            } else {
                                resultCount.textContent = ' - No results found';
                            }
                        } else {
                            statusText.textContent = 'Scraping in progress...';
                            resultCount.textContent = ` - Found ${data.result_count} results so far`;
                        }
                    })
                    .catch(error => {
                        console.error('Error polling status:', error);
                    });
                }, 2000);
            }
        });
    </script>
</body>
</html>
        ''')
    
    app.run(debug=True, host='0.0.0.0', port=5000)