#!/bin/bash

# Enhanced Business Scraper Runner
# Usage: ./scraper.sh [options] <query>

# Default values
HEADLESS=false
OUTPUT_CSV=false
OUTPUT_JSON=false
OUTPUT_EXCEL=false
SAVE_DB=false
RETRY_FAILED=false
MAX_RESULTS=20
MAX_PAGES=3
LOCATION=""
PROXIES=()
REFERENCE_COORDS=()

# Display help information
display_help() {
    echo "Usage: $0 [options] <query>"
    echo
    echo "Options:"
    echo "  -h, --help            Show this help message and exit"
    echo "  -l, --location LOC    Set the location to search in"
    echo "  -m, --max-results NUM  Maximum number of results to scrape (default: 20)"
    echo "  -p, --max-pages NUM    Maximum pages to crawl per website (default: 3)"
    echo "  -H, --headless        Run browser in headless mode"
    echo "  --csv                 Export results to CSV"
    echo "  --json                Export results to JSON"
    echo "  --excel               Export results to Excel"
    echo "  --db                  Save results to database"
    echo "  --retry               Retry failed websites"
    echo "  --proxy PROXY1 PROXY2  List of proxy servers to use"
    echo "  --coords LAT LNG       Reference coordinates for distance calculation"
    echo
    echo "Example:"
    echo "  $0 --location 'New York' --csv --json 'restaurants'"
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            display_help
            ;;
        -l|--location)
            LOCATION="$2"
            shift 2
            ;;
        -m|--max-results)
            MAX_RESULTS="$2"
            shift 2
            ;;
        -p|--max-pages)
            MAX_PAGES="$2"
            shift 2
            ;;
        -H|--headless)
            HEADLESS=true
            shift
            ;;
        --csv)
            OUTPUT_CSV=true
            shift
            ;;
        --json)
            OUTPUT_JSON=true
            shift
            ;;
        --excel)
            OUTPUT_EXCEL=true
            shift
            ;;
        --db)
            SAVE_DB=true
            shift
            ;;
        --retry)
            RETRY_FAILED=true
            shift
            ;;
        --proxy)
            shift
            while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
                PROXIES+=("$1")
                shift
            done
            ;;
        --coords)
            REFERENCE_COORDS=("$2" "$3")
            shift 3
            ;;
        *)
            QUERY="$1"
            shift
            ;;
    esac
done

# Check if query is provided
if [[ -z "$QUERY" ]]; then
    echo "Error: Search query is required."
    display_help
    exit 1
fi

# Build the command
COMMAND="python main.py \"$QUERY\""

# Add options to the command
if [[ -n "$LOCATION" ]]; then
    COMMAND+=" --location \"$LOCATION\""
fi

COMMAND+=" --max-results $MAX_RESULTS"
COMMAND+=" --max-pages $MAX_PAGES"

if [[ "$HEADLESS" = true ]]; then
    COMMAND+=" --headless"
fi

if [[ "$OUTPUT_CSV" = true ]]; then
    COMMAND+=" --output-csv"
fi

if [[ "$OUTPUT_JSON" = true ]]; then
    COMMAND+=" --output-json"
fi

if [[ "$OUTPUT_EXCEL" = true ]]; then
    COMMAND+=" --output-excel"
fi

if [[ "$SAVE_DB" = true ]]; then
    COMMAND+=" --save-db"
fi

if [[ "$RETRY_FAILED" = true ]]; then
    COMMAND+=" --retry-failed"
fi

if [[ ${#PROXIES[@]} -gt 0 ]]; then
    COMMAND+=" --proxies"
    for proxy in "${PROXIES[@]}"; do
        COMMAND+=" \"$proxy\""
    done
fi

if [[ ${#REFERENCE_COORDS[@]} -eq 2 ]]; then
    COMMAND+=" --reference-coords ${REFERENCE_COORDS[0]} ${REFERENCE_COORDS[1]}"
fi

# Execute the command
echo "Running command: $COMMAND"
eval "$COMMAND"