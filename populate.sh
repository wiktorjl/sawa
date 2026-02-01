#!/bin/bash
#
# Populate the stock database by downloading all data and uploading to PostgreSQL.
#
# This script orchestrates the complete workflow:
#   1. Download S&P 500 symbols
#   2. Download company overviews
#   3. Download stock prices
#   4. Download financial ratios
#   5. Download economy data
#   6. Download fundamentals (balance sheets, cash flows, income statements)
#   7. Combine fundamentals into consolidated files
#   8. Upload all data to PostgreSQL
#
# Usage:
#   ./populate.sh                    # Full populate
#   ./populate.sh --skip-download    # Upload existing data only
#   ./populate.sh --skip-upload      # Download only, don't upload
#   ./populate.sh --rebuild          # Rebuild database before upload
#
# Environment Variables:
#   DATABASE_URL      - PostgreSQL connection URL (required for upload)
#   POLYGON_API_KEY   - Polygon.io API key (required for prices/ratios)
#   MASSIVE_API_KEY   - Massive API key (required for fundamentals/overviews/economy)
#   POLYGON_S3_ACCESS_KEY - S3 access key (required for prices)
#   POLYGON_S3_SECRET_KEY - S3 secret key (required for prices)
#

set -e  # Exit on error

# Configuration
DATA_DIR="data"
SYMBOLS_FILE="$DATA_DIR/sp500_symbols.txt"
TRADING_DAYS_FILE="$DATA_DIR/trading_days_2021-01-01.txt"
OVERVIEWS_FILE="$DATA_DIR/overviews/OVERVIEWS.csv"
RATIOS_FILE="$DATA_DIR/ratios/RATIOS.csv"
PRICES_DIR="$DATA_DIR/prices"
FUNDAMENTALS_DIR="$DATA_DIR/fundamentals"
ECONOMY_DIR="$DATA_DIR/economy"

# Default options
SKIP_DOWNLOAD=false
SKIP_UPLOAD=false
REBUILD_DB=false
SKIP_PRICES=false
SKIP_FUNDAMENTALS=false
SKIP_ECONOMY=false
SKIP_RATIOS=false
SKIP_OVERVIEWS=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-download)
            SKIP_DOWNLOAD=true
            shift
            ;;
        --skip-upload)
            SKIP_UPLOAD=true
            shift
            ;;
        --rebuild)
            REBUILD_DB=true
            shift
            ;;
        --skip-prices)
            SKIP_PRICES=true
            shift
            ;;
        --skip-fundamentals)
            SKIP_FUNDAMENTALS=true
            shift
            ;;
        --skip-economy)
            SKIP_ECONOMY=true
            shift
            ;;
        --skip-ratios)
            SKIP_RATIOS=true
            shift
            ;;
        --skip-overviews)
            SKIP_OVERVIEWS=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --skip-download      Skip data download (upload existing data only)"
            echo "  --skip-upload        Skip database upload (download only)"
            echo "  --rebuild            Rebuild database schema before upload"
            echo "  --skip-prices        Skip downloading stock prices"
            echo "  --skip-fundamentals  Skip downloading fundamentals"
            echo "  --skip-economy       Skip downloading economy data"
            echo "  --skip-ratios        Skip downloading financial ratios"
            echo "  --skip-overviews     Skip downloading company overviews"
            echo "  -h, --help           Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  DATABASE_URL          PostgreSQL connection URL"
            echo "  POLYGON_API_KEY       Polygon.io API key"
            echo "  MASSIVE_API_KEY       Massive API key"
            echo "  POLYGON_S3_ACCESS_KEY S3 access key for prices"
            echo "  POLYGON_S3_SECRET_KEY S3 secret key for prices"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Print banner
print_banner() {
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║              S&P 500 Database Population Tool                ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
}

# Check if required environment variables are set
check_env() {
    local var_name=$1
    local var_value=$(eval echo "\$$var_name")
    if [[ -z "$var_value" ]]; then
        return 1
    fi
    return 0
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    local missing_deps=()
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        missing_deps+=("python3")
    fi
    
    # Check required Python packages
    if ! python3 -c "import psycopg" 2>/dev/null; then
        log_warning "psycopg not installed. Install with: pip install psycopg[binary]"
    fi
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        exit 1
    fi
    
    log_success "Prerequisites OK"
}

# Download S&P 500 symbols
download_symbols() {
    log_info "Step 1/7: Downloading S&P 500 symbols..."
    python3 download_sp500_symbols.py -o "$SYMBOLS_FILE"
    log_success "Downloaded symbols to $SYMBOLS_FILE"
}

# Download company overviews
download_overviews() {
    if [[ "$SKIP_OVERVIEWS" == true ]]; then
        log_info "Skipping company overviews download"
        return
    fi
    
    if ! check_env "MASSIVE_API_KEY"; then
        log_warning "MASSIVE_API_KEY not set - skipping overviews download"
        return
    fi
    
    log_info "Step 2/7: Downloading company overviews..."
    python3 download_overviews.py --symbols-file "$SYMBOLS_FILE" --continue
    log_success "Downloaded company overviews"
}

# Download stock prices
download_prices() {
    if [[ "$SKIP_PRICES" == true ]]; then
        log_info "Skipping stock prices download"
        return
    fi
    
    if ! check_env "POLYGON_S3_ACCESS_KEY" || ! check_env "POLYGON_S3_SECRET_KEY"; then
        log_warning "S3 credentials not set - skipping prices download"
        return
    fi
    
    log_info "Step 3/7: Downloading stock prices..."
    
    # Get the most recent trading days file
    local latest_trading_days=$(ls -t data/trading_days_*.txt 2>/dev/null | head -1)
    if [[ -z "$latest_trading_days" ]]; then
        log_warning "No trading days file found, using default date range"
        python3 download_daily_prices.py 2024-01-01 --days 365 --continue \
            --symbols "$SYMBOLS_FILE" -o "$PRICES_DIR"
    else
        log_info "Using trading days from: $latest_trading_days"
        python3 download_daily_prices.py 2024-01-01 --days 365 --continue \
            --symbols "$SYMBOLS_FILE" -o "$PRICES_DIR" \
            --trading-days "$latest_trading_days"
    fi
    
    log_success "Downloaded stock prices"
}

# Download financial ratios
download_ratios() {
    if [[ "$SKIP_RATIOS" == true ]]; then
        log_info "Skipping financial ratios download"
        return
    fi
    
    if ! check_env "POLYGON_API_KEY"; then
        log_warning "POLYGON_API_KEY not set - skipping ratios download"
        return
    fi
    
    log_info "Step 4/7: Downloading financial ratios..."
    python3 ratio_downloader.py "$SYMBOLS_FILE" -o "$RATIOS_FILE"
    log_success "Downloaded financial ratios"
}

# Download economy data
download_economy() {
    if [[ "$SKIP_ECONOMY" == true ]]; then
        log_info "Skipping economy data download"
        return
    fi
    
    if ! check_env "MASSIVE_API_KEY"; then
        log_warning "MASSIVE_API_KEY not set - skipping economy data download"
        return
    fi
    
    log_info "Step 5/7: Downloading economy data..."
    python3 download_economy_data.py --years 5 --continue --output-dir "$ECONOMY_DIR"
    log_success "Downloaded economy data"
}

# Download fundamentals
download_fundamentals() {
    if [[ "$SKIP_FUNDAMENTALS" == true ]]; then
        log_info "Skipping fundamentals download"
        return
    fi
    
    if ! check_env "MASSIVE_API_KEY"; then
        log_warning "MASSIVE_API_KEY not set - skipping fundamentals download"
        return
    fi
    
    log_info "Step 6/7: Downloading fundamentals..."
    
    # Download balance sheets
    log_info "  Downloading balance sheets..."
    python3 download_fundamentals.py --endpoint balance-sheets \
        --symbols-file "$SYMBOLS_FILE" --years 5 --continue \
        --output-dir "$FUNDAMENTALS_DIR"
    
    # Download cash flow statements
    log_info "  Downloading cash flow statements..."
    python3 download_fundamentals.py --endpoint cash-flow \
        --symbols-file "$SYMBOLS_FILE" --years 5 --continue \
        --output-dir "$FUNDAMENTALS_DIR"
    
    # Download income statements
    log_info "  Downloading income statements..."
    python3 download_fundamentals.py --endpoint income-statements \
        --symbols-file "$SYMBOLS_FILE" --years 5 --continue \
        --output-dir "$FUNDAMENTALS_DIR"
    
    log_success "Downloaded fundamentals"
}

# Combine fundamentals into consolidated files
combine_fundamentals() {
    # Check if there are any per-ticker fundamental files to combine
    local fund_files=$(ls -1 "$FUNDAMENTALS_DIR"/*_balance_sheets.csv \
        "$FUNDAMENTALS_DIR"/*_cash_flow.csv \
        "$FUNDAMENTALS_DIR"/*_income_statements.csv 2>/dev/null | wc -l)
    
    if [[ "$fund_files" -eq 0 ]]; then
        log_warning "No fundamental files found to combine"
        return
    fi
    
    log_info "Combining fundamentals ($fund_files per-ticker files)..."
    python3 combine_fundamentals.py --input-dir "$FUNDAMENTALS_DIR" \
        --output-dir "$FUNDAMENTALS_DIR"
    log_success "Combined fundamentals"
}

# Download all data
download_data() {
    if [[ "$SKIP_DOWNLOAD" == true ]]; then
        log_info "Skipping download phase (--skip-download specified)"
        return
    fi
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                    DOWNLOAD PHASE                            ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    
    download_symbols
    download_overviews
    download_prices
    download_ratios
    download_economy
    download_fundamentals
    
    log_success "Download phase complete!"
}

# Upload data to PostgreSQL
upload_data() {
    if [[ "$SKIP_UPLOAD" == true ]]; then
        log_info "Skipping upload phase (--skip-upload specified)"
        return
    fi
    
    if ! check_env "DATABASE_URL"; then
        log_error "DATABASE_URL environment variable not set"
        log_error "Set it like: export DATABASE_URL=postgresql://user:pass@host:5432/db"
        exit 1
    fi
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                     UPLOAD PHASE                             ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Rebuild database if requested
    if [[ "$REBUILD_DB" == true ]]; then
        log_info "Rebuilding database schema..."
        python3 rebuild_database.py --force --drop
        log_success "Database rebuilt"
    fi
    
    # Set PostgreSQL environment variables from DATABASE_URL
    log_info "Configuring database connection..."
    export PGHOST=$(echo "$DATABASE_URL" | sed -n 's/.*@\([^:]*\):.*/\1/p')
    export PGPORT=$(echo "$DATABASE_URL" | sed -n 's/.*:\([0-9]*\)\/.*/\1/p')
    export PGDATABASE=$(echo "$DATABASE_URL" | sed -n 's/.*\/\([^?]*\).*/\1/p')
    export PGUSER=$(echo "$DATABASE_URL" | sed -n 's/.*:\/\/\([^:]*\):.*/\1/p')
    export PGPASSWORD=$(echo "$DATABASE_URL" | sed -n 's/.*:\/\/[^:]*:\([^@]*\)@.*/\1/p')
    
    # Upload companies first (required for foreign key constraints)
    if [[ -f "$OVERVIEWS_FILE" ]]; then
        log_info "Uploading companies..."
        python3 load_csv_to_postgres.py --mapping data_mappings.json \
            --tables companies --upsert -v
        log_success "Uploaded companies"
    fi
    
    # Upload economy data (no foreign key dependencies)
    log_info "Uploading economy data..."
    python3 load_csv_to_postgres.py --mapping data_mappings.json \
        --tables treasury_yields,inflation,inflation_expectations,labor_market --upsert
    log_success "Uploaded economy data"
    
    # Upload financial ratios
    if [[ -f "$RATIOS_FILE" ]]; then
        log_info "Uploading financial ratios..."
        python3 load_csv_to_postgres.py --mapping data_mappings.json \
            --tables financial_ratios --upsert
        log_success "Uploaded financial ratios"
    fi
    
    # Upload fundamentals
    if [[ -f "$FUNDAMENTALS_DIR/balance_sheets.csv" ]]; then
        log_info "Uploading balance sheets..."
        python3 load_csv_to_postgres.py --mapping data_mappings.json \
            --tables balance_sheets --upsert
        log_success "Uploaded balance sheets"
    fi
    
    if [[ -f "$FUNDAMENTALS_DIR/income_statements.csv" ]]; then
        log_info "Uploading income statements..."
        python3 load_csv_to_postgres.py --mapping data_mappings.json \
            --tables income_statements --upsert
        log_success "Uploaded income statements"
    fi
    
    if [[ -f "$FUNDAMENTALS_DIR/cash_flow.csv" ]]; then
        log_info "Uploading cash flows..."
        python3 load_csv_to_postgres.py --mapping data_mappings.json \
            --tables cash_flows --upsert
        log_success "Uploaded cash flows"
    fi
    
    # Upload stock prices (per-symbol CSVs)
    if [[ -d "$PRICES_DIR" ]]; then
        log_info "Uploading stock prices..."
        local price_count=$(ls -1 "$PRICES_DIR"/*.csv 2>/dev/null | wc -l)
        log_info "Found $price_count price files"
        
        # Create a temporary combined file for prices
        log_info "Combining price files for upload..."
        python3 -c "
import csv
import os
from pathlib import Path

prices_dir = Path('$PRICES_DIR')
output_file = Path('$DATA_DIR/prices_combined.csv')

all_rows = []
output_headers = ['date', 'ticker', 'open', 'close', 'high', 'low', 'volume']

for csv_file in sorted(prices_dir.glob('*.csv')):
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Map symbol to ticker and reorder columns
            mapped_row = {
                'date': row.get('date', ''),
                'ticker': row.get('symbol', ''),
                'open': row.get('open', ''),
                'close': row.get('close', ''),
                'high': row.get('high', ''),
                'low': row.get('low', ''),
                'volume': row.get('volume', '')
            }
            all_rows.append(mapped_row)

# Write combined file
with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    writer.writerows(all_rows)

print(f'Combined {len(all_rows)} price records')
"
        
        # Upload combined prices
        log_info "Uploading combined prices..."
        python3 load_csv_to_postgres.py \
            --csv "$DATA_DIR/prices_combined.csv" \
            --table stock_prices \
            --columns date,ticker,open,close,high,low,volume \
            --upsert
        
        rm -f "$DATA_DIR/prices_combined.csv"
        log_success "Uploaded stock prices"
    fi
    
    log_success "Upload phase complete!"
}

# Main execution
main() {
    print_banner
    
    # Show configuration
    log_info "Configuration:"
    log_info "  Data directory: $DATA_DIR"
    log_info "  Symbols file: $SYMBOLS_FILE"
    log_info "  Skip download: $SKIP_DOWNLOAD"
    log_info "  Skip upload: $SKIP_UPLOAD"
    log_info "  Rebuild DB: $REBUILD_DB"
    echo ""
    
    # Check prerequisites
    check_prerequisites
    
    # Create data directories
    mkdir -p "$DATA_DIR" "$DATA_DIR/overviews" "$DATA_DIR/ratios" \
        "$PRICES_DIR" "$FUNDAMENTALS_DIR" "$ECONOMY_DIR"
    
    # Run phases
    download_data
    combine_fundamentals
    upload_data
    
    echo ""
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                  POPULATION COMPLETE!                        ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""
    log_success "All data has been downloaded and uploaded to the database"
}

# Run main function
main
