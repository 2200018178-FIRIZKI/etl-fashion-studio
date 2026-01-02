"""
Unit tests for the load module.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, mock_open
import sys
import os
import tempfile

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.load import (
    load_to_csv,
    load_to_google_sheets,
    load_to_postgresql,
    load_data
)


# Sample DataFrame for testing
def get_sample_dataframe():
    """Return a sample DataFrame for testing."""
    return pd.DataFrame({
        'Title': ['T-shirt 2', 'Hoodie 3'],
        'Price': [1634400.0, 7950080.0],
        'Rating': [3.9, 4.8],
        'Colors': [3, 3],
        'Size': ['M', 'L'],
        'Gender': ['Women', 'Unisex'],
        'timestamp': ['2025-01-02 10:00:00', '2025-01-02 10:00:00']
    })


class TestLoadToCSV:
    """Test cases for load_to_csv function."""
    
    def test_load_to_csv_success(self):
        """Test successful CSV save."""
        df = get_sample_dataframe()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Patch os.path.dirname to return temp directory
            with patch('utils.load.os.path.dirname', return_value=tmpdir):
                filepath = load_to_csv(df, "test_products.csv")
                
                assert os.path.exists(filepath)
                
                # Read back and verify
                loaded_df = pd.read_csv(filepath)
                assert len(loaded_df) == 2
                assert 'Title' in loaded_df.columns
    
    def test_load_to_csv_empty_dataframe(self):
        """Test load_to_csv with empty DataFrame."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_csv(df, "test.csv")
        assert "empty" in str(excinfo.value)
    
    def test_load_to_csv_none_dataframe(self):
        """Test load_to_csv with None DataFrame."""
        with pytest.raises(ValueError) as excinfo:
            load_to_csv(None, "test.csv")
        assert "empty" in str(excinfo.value)
    
    def test_load_to_csv_invalid_filename(self):
        """Test load_to_csv with invalid filename."""
        df = get_sample_dataframe()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_csv(df, "test.txt")
        assert ".csv" in str(excinfo.value)
    
    def test_load_to_csv_none_filename(self):
        """Test load_to_csv with None filename."""
        df = get_sample_dataframe()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_csv(df, None)
        assert "string" in str(excinfo.value)


class TestLoadToGoogleSheets:
    """Test cases for load_to_google_sheets function."""
    
    @patch('utils.load.build')
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.os.path.exists')
    def test_load_to_google_sheets_success(self, mock_exists, mock_creds, mock_build):
        """Test successful Google Sheets save."""
        df = get_sample_dataframe()
        
        mock_exists.return_value = True
        mock_creds.return_value = MagicMock()
        
        # Mock Google Sheets service
        mock_service = MagicMock()
        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value.values.return_value.clear.return_value.execute.return_value = {}
        mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {
            'updatedCells': 14
        }
        
        result = load_to_google_sheets(
            df, 
            "test_spreadsheet_id",
            "Sheet1",
            "credentials.json"
        )
        
        assert result is True
    
    def test_load_to_google_sheets_empty_dataframe(self):
        """Test load_to_google_sheets with empty DataFrame."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_google_sheets(df, "test_id")
        assert "empty" in str(excinfo.value)
    
    def test_load_to_google_sheets_none_dataframe(self):
        """Test load_to_google_sheets with None DataFrame."""
        with pytest.raises(ValueError) as excinfo:
            load_to_google_sheets(None, "test_id")
        assert "empty" in str(excinfo.value)
    
    def test_load_to_google_sheets_invalid_spreadsheet_id(self):
        """Test load_to_google_sheets with invalid spreadsheet ID."""
        df = get_sample_dataframe()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_google_sheets(df, "")
        assert "spreadsheet_id" in str(excinfo.value)
    
    def test_load_to_google_sheets_none_spreadsheet_id(self):
        """Test load_to_google_sheets with None spreadsheet ID."""
        df = get_sample_dataframe()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_google_sheets(df, None)
        assert "spreadsheet_id" in str(excinfo.value)
    
    @patch('utils.load.os.path.exists')
    def test_load_to_google_sheets_missing_credentials(self, mock_exists):
        """Test load_to_google_sheets with missing credentials file."""
        df = get_sample_dataframe()
        mock_exists.return_value = False
        
        with pytest.raises(FileNotFoundError) as excinfo:
            load_to_google_sheets(df, "test_id", credentials_file="missing.json")
        assert "not found" in str(excinfo.value)


class TestLoadToPostgreSQL:
    """Test cases for load_to_postgresql function."""
    
    @patch('utils.load.create_engine')
    def test_load_to_postgresql_success(self, mock_create_engine):
        """Test successful PostgreSQL save."""
        df = get_sample_dataframe()
        
        # Mock engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.execute.return_value.scalar.return_value = 2
        
        # Mock to_sql
        with patch.object(pd.DataFrame, 'to_sql'):
            result = load_to_postgresql(
                df, 
                "test_products",
                db_url="postgresql://user:pass@localhost:5432/testdb"
            )
        
        assert result is True
    
    def test_load_to_postgresql_empty_dataframe(self):
        """Test load_to_postgresql with empty DataFrame."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_postgresql(df, "test_table")
        assert "empty" in str(excinfo.value)
    
    def test_load_to_postgresql_none_dataframe(self):
        """Test load_to_postgresql with None DataFrame."""
        with pytest.raises(ValueError) as excinfo:
            load_to_postgresql(None, "test_table")
        assert "empty" in str(excinfo.value)
    
    def test_load_to_postgresql_invalid_table_name(self):
        """Test load_to_postgresql with invalid table name."""
        df = get_sample_dataframe()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_postgresql(df, "")
        assert "table_name" in str(excinfo.value)
    
    def test_load_to_postgresql_none_table_name(self):
        """Test load_to_postgresql with None table name."""
        df = get_sample_dataframe()
        
        with pytest.raises(ValueError) as excinfo:
            load_to_postgresql(df, None)
        assert "table_name" in str(excinfo.value)
    
    @patch('utils.load.create_engine')
    def test_load_to_postgresql_connection_error(self, mock_create_engine):
        """Test load_to_postgresql with connection error."""
        df = get_sample_dataframe()
        
        mock_create_engine.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as excinfo:
            load_to_postgresql(df, "test_table", db_url="postgresql://invalid")
        assert "Failed to save to PostgreSQL" in str(excinfo.value)


class TestLoadData:
    """Test cases for load_data function."""
    
    @patch('utils.load.load_to_csv')
    def test_load_data_csv_only(self, mock_csv):
        """Test load_data with CSV only."""
        df = get_sample_dataframe()
        mock_csv.return_value = "/path/to/products.csv"
        
        result = load_data(df, csv_filename="products.csv")
        
        assert 'csv' in result
        assert result['csv']['success'] is True
        mock_csv.assert_called_once()
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_google_sheets')
    def test_load_data_csv_and_sheets(self, mock_sheets, mock_csv):
        """Test load_data with CSV and Google Sheets."""
        df = get_sample_dataframe()
        mock_csv.return_value = "/path/to/products.csv"
        mock_sheets.return_value = True
        
        result = load_data(
            df, 
            csv_filename="products.csv",
            google_sheets_id="test_id"
        )
        
        assert 'csv' in result
        assert 'google_sheets' in result
        assert result['csv']['success'] is True
        assert result['google_sheets']['success'] is True
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_postgresql')
    def test_load_data_csv_and_postgresql(self, mock_postgres, mock_csv):
        """Test load_data with CSV and PostgreSQL."""
        df = get_sample_dataframe()
        mock_csv.return_value = "/path/to/products.csv"
        mock_postgres.return_value = True
        
        result = load_data(
            df, 
            csv_filename="products.csv",
            postgresql_url="postgresql://user:pass@localhost/db"
        )
        
        assert 'csv' in result
        assert 'postgresql' in result
        assert result['csv']['success'] is True
        assert result['postgresql']['success'] is True
    
    @patch('utils.load.load_to_csv')
    @patch('utils.load.load_to_google_sheets')
    @patch('utils.load.load_to_postgresql')
    def test_load_data_all_destinations(self, mock_postgres, mock_sheets, mock_csv):
        """Test load_data with all destinations."""
        df = get_sample_dataframe()
        mock_csv.return_value = "/path/to/products.csv"
        mock_sheets.return_value = True
        mock_postgres.return_value = True
        
        result = load_data(
            df, 
            csv_filename="products.csv",
            google_sheets_id="test_id",
            postgresql_url="postgresql://user:pass@localhost/db"
        )
        
        assert 'csv' in result
        assert 'google_sheets' in result
        assert 'postgresql' in result
        assert all(r['success'] for r in result.values())
    
    def test_load_data_empty_dataframe(self):
        """Test load_data with empty DataFrame."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError) as excinfo:
            load_data(df)
        assert "empty" in str(excinfo.value)
    
    def test_load_data_none_dataframe(self):
        """Test load_data with None DataFrame."""
        with pytest.raises(ValueError) as excinfo:
            load_data(None)
        assert "empty" in str(excinfo.value)
    
    @patch('utils.load.load_to_csv')
    def test_load_data_csv_failure_handled(self, mock_csv):
        """Test that CSV failure is handled gracefully."""
        df = get_sample_dataframe()
        mock_csv.side_effect = IOError("Write error")
        
        result = load_data(df, csv_filename="products.csv")
        
        assert 'csv' in result
        assert result['csv']['success'] is False
        assert 'error' in result['csv']


class TestLoadIntegration:
    """Integration tests for load module."""
    
    def test_csv_roundtrip(self):
        """Test saving and loading CSV."""
        df = get_sample_dataframe()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('utils.load.os.path.dirname', return_value=tmpdir):
                filepath = load_to_csv(df, "roundtrip_test.csv")
                
                loaded_df = pd.read_csv(filepath)
                
                assert len(loaded_df) == len(df)
                assert list(loaded_df.columns) == list(df.columns)
                assert loaded_df['Title'].tolist() == df['Title'].tolist()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
