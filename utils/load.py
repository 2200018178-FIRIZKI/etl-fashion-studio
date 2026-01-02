"""
Load module for Fashion Studio ETL Pipeline.
This module handles saving data to various storage destinations:
- CSV files
- Google Sheets
- PostgreSQL database
"""

import pandas as pd
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text


def load_to_csv(df: pd.DataFrame, filename: str = "products.csv") -> str:
    """
    Save DataFrame to a CSV file.
    
    Args:
        df: DataFrame to save
        filename: Name of the output CSV file
        
    Returns:
        Path to the saved file
        
    Raises:
        ValueError: If DataFrame is invalid
        IOError: If file cannot be written
    """
    try:
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        if not isinstance(filename, str) or not filename.endswith('.csv'):
            raise ValueError("Filename must be a string ending with '.csv'")
        
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filepath = os.path.join(current_dir, filename)
        
        df.to_csv(filepath, index=False)
        print(f"Data successfully saved to {filepath}")
        
        return filepath
        
    except ValueError as e:
        raise ValueError(f"Validation error: {str(e)}")
    except IOError as e:
        raise IOError(f"Error writing to file: {str(e)}")
    except Exception as e:
        raise Exception(f"Failed to save to CSV: {str(e)}")


def load_to_google_sheets(
    df: pd.DataFrame,
    spreadsheet_id: str,
    sheet_name: str = "Sheet1",
    credentials_file: str = "google-sheets-api.json"
) -> bool:
    """
    Save DataFrame to Google Sheets.
    
    Args:
        df: DataFrame to save
        spreadsheet_id: Google Sheets spreadsheet ID
        sheet_name: Name of the sheet to write to
        credentials_file: Path to the service account credentials JSON file
        
    Returns:
        True if successful
        
    Raises:
        ValueError: If DataFrame or parameters are invalid
        FileNotFoundError: If credentials file not found
        Exception: If Google Sheets API fails
    """
    try:
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        if not spreadsheet_id or not isinstance(spreadsheet_id, str):
            raise ValueError("Invalid spreadsheet_id")
        
        # Get credentials file path
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        creds_path = os.path.join(current_dir, credentials_file)
        
        if not os.path.exists(creds_path):
            raise FileNotFoundError(f"Credentials file not found: {creds_path}")
        
        # Authenticate with Google Sheets API
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_file(
            creds_path, scopes=scopes
        )
        
        service = build('sheets', 'v4', credentials=credentials)
        
        # Prepare data for Google Sheets
        # Convert DataFrame to list of lists (including header)
        values = [df.columns.tolist()] + df.values.tolist()
        
        # Clear existing content and write new data
        range_name = f"{sheet_name}!A1"
        
        # Clear the sheet first
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        # Write new data
        body = {'values': values}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"Data successfully saved to Google Sheets: {result.get('updatedCells')} cells updated")
        return True
        
    except ValueError as e:
        raise ValueError(f"Validation error: {str(e)}")
    except FileNotFoundError as e:
        raise FileNotFoundError(str(e))
    except Exception as e:
        raise Exception(f"Failed to save to Google Sheets: {str(e)}")


def load_to_postgresql(
    df: pd.DataFrame,
    table_name: str = "products",
    db_url: str = None,
    host: str = "localhost",
    port: int = 5432,
    database: str = "fashion_studio",
    username: str = "postgres",
    password: str = "password"
) -> bool:
    """
    Save DataFrame to PostgreSQL database.
    
    Args:
        df: DataFrame to save
        table_name: Name of the database table
        db_url: Full database URL (overrides other connection params)
        host: Database host
        port: Database port
        database: Database name
        username: Database username
        password: Database password
        
    Returns:
        True if successful
        
    Raises:
        ValueError: If DataFrame is invalid
        Exception: If database operation fails
    """
    try:
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table_name")
        
        # Create database connection URL
        if db_url:
            connection_url = db_url
        else:
            connection_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        
        # Create engine and connect
        engine = create_engine(connection_url)
        
        # Test connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        
        # Write DataFrame to database
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # Replace table if exists
            index=False
        )
        
        # Verify data was written
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
        
        print(f"Data successfully saved to PostgreSQL table '{table_name}': {count} rows")
        engine.dispose()
        
        return True
        
    except ValueError as e:
        raise ValueError(f"Validation error: {str(e)}")
    except Exception as e:
        raise Exception(f"Failed to save to PostgreSQL: {str(e)}")


def load_data(
    df: pd.DataFrame,
    csv_filename: str = "products.csv",
    google_sheets_id: str = None,
    google_sheets_name: str = "Sheet1",
    google_credentials: str = "google-sheets-api.json",
    postgresql_url: str = None,
    postgresql_table: str = "products"
) -> dict:
    """
    Main load function to save data to multiple destinations.
    
    Args:
        df: DataFrame to save
        csv_filename: Name of the output CSV file
        google_sheets_id: Google Sheets spreadsheet ID (optional)
        google_sheets_name: Name of the sheet
        google_credentials: Path to Google credentials file
        postgresql_url: PostgreSQL connection URL (optional)
        postgresql_table: Name of the PostgreSQL table
        
    Returns:
        Dictionary with status of each load operation
        
    Raises:
        ValueError: If DataFrame is invalid
    """
    try:
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        results = {}
        
        # Always save to CSV
        try:
            csv_path = load_to_csv(df, csv_filename)
            results['csv'] = {'success': True, 'path': csv_path}
        except Exception as e:
            results['csv'] = {'success': False, 'error': str(e)}
        
        # Save to Google Sheets if ID provided
        if google_sheets_id:
            try:
                load_to_google_sheets(
                    df, 
                    google_sheets_id, 
                    google_sheets_name,
                    google_credentials
                )
                results['google_sheets'] = {'success': True, 'spreadsheet_id': google_sheets_id}
            except Exception as e:
                results['google_sheets'] = {'success': False, 'error': str(e)}
        
        # Save to PostgreSQL if URL provided
        if postgresql_url:
            try:
                load_to_postgresql(df, postgresql_table, db_url=postgresql_url)
                results['postgresql'] = {'success': True, 'table': postgresql_table}
            except Exception as e:
                results['postgresql'] = {'success': False, 'error': str(e)}
        
        return results
        
    except ValueError as e:
        raise ValueError(f"Load failed: {str(e)}")
    except Exception as e:
        raise Exception(f"Load failed: {str(e)}")


if __name__ == "__main__":
    # Test loading with sample data
    sample_data = {
        'Title': ['T-shirt 2', 'Hoodie 3'],
        'Price': [1634400.0, 7950080.0],
        'Rating': [3.9, 4.8],
        'Colors': [3, 3],
        'Size': ['M', 'L'],
        'Gender': ['Women', 'Unisex'],
        'timestamp': ['2025-01-02 10:00:00', '2025-01-02 10:00:00']
    }
    
    df = pd.DataFrame(sample_data)
    
    # Test CSV save
    results = load_data(df, csv_filename="test_products.csv")
    print(results)
