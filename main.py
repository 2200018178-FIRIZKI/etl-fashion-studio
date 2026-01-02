"""
Fashion Studio ETL Pipeline - Main Orchestrator

This script orchestrates the complete ETL pipeline:
1. Extract: Scrape product data from Fashion Studio website
2. Transform: Clean and transform the data
3. Load: Save data to CSV, Google Sheets, and/or PostgreSQL

Usage:
    python main.py
    
Author: Data Engineer
Date: 2025-01-02
"""

import argparse
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.extract import extract_data
from utils.transform import transform_data, validate_transformed_data
from utils.load import load_data


def run_etl_pipeline(
    start_page: int = 1,
    end_page: int = 50,
    csv_filename: str = "products.csv",
    google_sheets_id: str = None,
    google_sheets_name: str = "Sheet1",
    google_credentials: str = "google-sheets-api.json",
    postgresql_url: str = None,
    postgresql_table: str = "products"
) -> dict:
    """
    Run the complete ETL pipeline.
    
    Args:
        start_page: First page to scrape (default: 1)
        end_page: Last page to scrape (default: 50)
        csv_filename: Output CSV filename
        google_sheets_id: Google Sheets spreadsheet ID (optional)
        google_sheets_name: Sheet name in Google Sheets
        google_credentials: Path to Google credentials JSON
        postgresql_url: PostgreSQL connection URL (optional)
        postgresql_table: PostgreSQL table name
        
    Returns:
        Dictionary with pipeline results
    """
    results = {
        'extract': {'success': False},
        'transform': {'success': False},
        'load': {'success': False}
    }
    
    try:
        # ==================== EXTRACT ====================
        print("=" * 60)
        print("STEP 1: EXTRACTING DATA")
        print("=" * 60)
        
        raw_data = extract_data(start_page, end_page)
        results['extract'] = {
            'success': True,
            'records_extracted': len(raw_data)
        }
        print(f"Extraction complete: {len(raw_data)} records extracted\n")
        
        # ==================== TRANSFORM ====================
        print("=" * 60)
        print("STEP 2: TRANSFORMING DATA")
        print("=" * 60)
        
        clean_df = transform_data(raw_data)
        validate_transformed_data(clean_df)
        
        results['transform'] = {
            'success': True,
            'records_after_cleaning': len(clean_df),
            'columns': list(clean_df.columns),
            'dtypes': {col: str(dtype) for col, dtype in clean_df.dtypes.items()}
        }
        print(f"Transformation complete: {len(clean_df)} clean records\n")
        
        # Display sample data
        print("Sample transformed data:")
        print(clean_df.head())
        print(f"\nData types:\n{clean_df.dtypes}\n")
        
        # ==================== LOAD ====================
        print("=" * 60)
        print("STEP 3: LOADING DATA")
        print("=" * 60)
        
        load_results = load_data(
            df=clean_df,
            csv_filename=csv_filename,
            google_sheets_id=google_sheets_id,
            google_sheets_name=google_sheets_name,
            google_credentials=google_credentials,
            postgresql_url=postgresql_url,
            postgresql_table=postgresql_table
        )
        
        results['load'] = {
            'success': True,
            'destinations': load_results
        }
        print(f"Load complete!\n")
        
        # ==================== SUMMARY ====================
        print("=" * 60)
        print("ETL PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Total pages scraped: {end_page - start_page + 1}")
        print(f"Records extracted: {results['extract']['records_extracted']}")
        print(f"Records after cleaning: {results['transform']['records_after_cleaning']}")
        print(f"Records removed: {results['extract']['records_extracted'] - results['transform']['records_after_cleaning']}")
        print("\nLoad destinations:")
        for dest, status in load_results.items():
            if status.get('success'):
                print(f"  ✓ {dest}: Success")
            else:
                print(f"  ✗ {dest}: Failed - {status.get('error', 'Unknown error')}")
        
        return results
        
    except Exception as e:
        print(f"\nETL Pipeline Error: {str(e)}")
        results['error'] = str(e)
        return results


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Fashion Studio ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --pages 1 10 --csv output.csv
  python main.py --google-sheets YOUR_SPREADSHEET_ID
  python main.py --postgresql postgresql://user:pass@localhost:5432/db
        """
    )
    
    parser.add_argument(
        '--pages',
        nargs=2,
        type=int,
        default=[1, 50],
        metavar=('START', 'END'),
        help='Page range to scrape (default: 1 50)'
    )
    
    parser.add_argument(
        '--csv',
        type=str,
        default='products.csv',
        help='Output CSV filename (default: products.csv)'
    )
    
    parser.add_argument(
        '--google-sheets',
        type=str,
        default=None,
        help='Google Sheets spreadsheet ID'
    )
    
    parser.add_argument(
        '--sheet-name',
        type=str,
        default='Sheet1',
        help='Google Sheets sheet name (default: Sheet1)'
    )
    
    parser.add_argument(
        '--google-credentials',
        type=str,
        default='google-sheets-api.json',
        help='Path to Google credentials JSON (default: google-sheets-api.json)'
    )
    
    parser.add_argument(
        '--postgresql',
        type=str,
        default=None,
        help='PostgreSQL connection URL'
    )
    
    parser.add_argument(
        '--table',
        type=str,
        default='products',
        help='PostgreSQL table name (default: products)'
    )
    
    args = parser.parse_args()
    
    # Run the pipeline
    results = run_etl_pipeline(
        start_page=args.pages[0],
        end_page=args.pages[1],
        csv_filename=args.csv,
        google_sheets_id=args.google_sheets,
        google_sheets_name=args.sheet_name,
        google_credentials=args.google_credentials,
        postgresql_url=args.postgresql,
        postgresql_table=args.table
    )
    
    # Exit with appropriate code
    if results.get('load', {}).get('success'):
        print("\n✓ ETL Pipeline completed successfully!")
        sys.exit(0)
    else:
        print("\n✗ ETL Pipeline failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
