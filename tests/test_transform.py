"""
Unit tests for the transform module.
"""

import pytest
import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.transform import (
    clean_price,
    clean_rating,
    clean_colors,
    clean_size,
    clean_gender,
    transform_data,
    validate_transformed_data,
    USD_TO_IDR_RATE
)


class TestCleanPrice:
    """Test cases for clean_price function."""
    
    def test_clean_price_valid(self):
        """Test cleaning valid price string."""
        result = clean_price("$102.15")
        expected = 102.15 * USD_TO_IDR_RATE
        assert result == expected
    
    def test_clean_price_with_cents(self):
        """Test cleaning price with cents."""
        result = clean_price("$496.88")
        expected = 496.88 * USD_TO_IDR_RATE
        assert result == expected
    
    def test_clean_price_whole_number(self):
        """Test cleaning whole number price."""
        result = clean_price("$100")
        expected = 100 * USD_TO_IDR_RATE
        assert result == expected
    
    def test_clean_price_unavailable(self):
        """Test cleaning 'Price Unavailable'."""
        result = clean_price("Price Unavailable")
        assert result is None
    
    def test_clean_price_none(self):
        """Test cleaning None price."""
        result = clean_price(None)
        assert result is None
    
    def test_clean_price_empty_string(self):
        """Test cleaning empty string."""
        result = clean_price("")
        assert result is None
    
    def test_clean_price_with_comma(self):
        """Test cleaning price with comma."""
        result = clean_price("$1,234.56")
        expected = 1234.56 * USD_TO_IDR_RATE
        assert result == expected
    
    def test_clean_price_without_dollar_sign(self):
        """Test cleaning price without dollar sign."""
        result = clean_price("102.15")
        expected = 102.15 * USD_TO_IDR_RATE
        assert result == expected
    
    def test_clean_price_with_whitespace(self):
        """Test cleaning price with whitespace."""
        result = clean_price("  $102.15  ")
        expected = 102.15 * USD_TO_IDR_RATE
        assert result == expected


class TestCleanRating:
    """Test cases for clean_rating function."""
    
    def test_clean_rating_valid(self):
        """Test cleaning valid rating string."""
        result = clean_rating("Rating: ⭐ 3.9 / 5")
        assert result == 3.9
    
    def test_clean_rating_whole_number(self):
        """Test cleaning whole number rating."""
        result = clean_rating("Rating: ⭐ 4 / 5")
        assert result == 4.0
    
    def test_clean_rating_high(self):
        """Test cleaning high rating."""
        result = clean_rating("Rating: ⭐ 4.8 / 5")
        assert result == 4.8
    
    def test_clean_rating_invalid(self):
        """Test cleaning 'Invalid Rating'."""
        result = clean_rating("Rating: ⭐ Invalid Rating / 5")
        assert result is None
    
    def test_clean_rating_not_rated(self):
        """Test cleaning 'Not Rated'."""
        result = clean_rating("Rating: Not Rated")
        assert result is None
    
    def test_clean_rating_none(self):
        """Test cleaning None rating."""
        result = clean_rating(None)
        assert result is None
    
    def test_clean_rating_empty_string(self):
        """Test cleaning empty string."""
        result = clean_rating("")
        assert result is None
    
    def test_clean_rating_with_whitespace(self):
        """Test cleaning rating with whitespace."""
        result = clean_rating("  Rating: ⭐ 3.9 / 5  ")
        assert result == 3.9


class TestCleanColors:
    """Test cases for clean_colors function."""
    
    def test_clean_colors_valid(self):
        """Test cleaning valid colors string."""
        result = clean_colors("3 Colors")
        assert result == 3
    
    def test_clean_colors_multiple(self):
        """Test cleaning multiple colors."""
        result = clean_colors("5 Colors")
        assert result == 5
    
    def test_clean_colors_eight(self):
        """Test cleaning 8 colors."""
        result = clean_colors("8 Colors")
        assert result == 8
    
    def test_clean_colors_none(self):
        """Test cleaning None colors."""
        result = clean_colors(None)
        assert result is None
    
    def test_clean_colors_empty_string(self):
        """Test cleaning empty string."""
        result = clean_colors("")
        assert result is None
    
    def test_clean_colors_with_whitespace(self):
        """Test cleaning colors with whitespace."""
        result = clean_colors("  3 Colors  ")
        assert result == 3


class TestCleanSize:
    """Test cases for clean_size function."""
    
    def test_clean_size_m(self):
        """Test cleaning size M."""
        result = clean_size("Size: M")
        assert result == "M"
    
    def test_clean_size_l(self):
        """Test cleaning size L."""
        result = clean_size("Size: L")
        assert result == "L"
    
    def test_clean_size_s(self):
        """Test cleaning size S."""
        result = clean_size("Size: S")
        assert result == "S"
    
    def test_clean_size_xl(self):
        """Test cleaning size XL."""
        result = clean_size("Size: XL")
        assert result == "XL"
    
    def test_clean_size_xxl(self):
        """Test cleaning size XXL."""
        result = clean_size("Size: XXL")
        assert result == "XXL"
    
    def test_clean_size_none(self):
        """Test cleaning None size."""
        result = clean_size(None)
        assert result is None
    
    def test_clean_size_empty_string(self):
        """Test cleaning empty string."""
        result = clean_size("")
        assert result is None
    
    def test_clean_size_with_whitespace(self):
        """Test cleaning size with whitespace."""
        result = clean_size("  Size: M  ")
        assert result == "M"


class TestCleanGender:
    """Test cases for clean_gender function."""
    
    def test_clean_gender_men(self):
        """Test cleaning gender Men."""
        result = clean_gender("Gender: Men")
        assert result == "Men"
    
    def test_clean_gender_women(self):
        """Test cleaning gender Women."""
        result = clean_gender("Gender: Women")
        assert result == "Women"
    
    def test_clean_gender_unisex(self):
        """Test cleaning gender Unisex."""
        result = clean_gender("Gender: Unisex")
        assert result == "Unisex"
    
    def test_clean_gender_none(self):
        """Test cleaning None gender."""
        result = clean_gender(None)
        assert result is None
    
    def test_clean_gender_empty_string(self):
        """Test cleaning empty string."""
        result = clean_gender("")
        assert result is None
    
    def test_clean_gender_with_whitespace(self):
        """Test cleaning gender with whitespace."""
        result = clean_gender("  Gender: Men  ")
        assert result == "Men"


class TestTransformData:
    """Test cases for transform_data function."""
    
    def test_transform_data_success(self):
        """Test successful data transformation."""
        sample_data = [
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            }
        ]
        
        df = transform_data(sample_data)
        
        assert len(df) == 1
        assert df.iloc[0]['Title'] == 'T-shirt 2'
        assert df.iloc[0]['Price'] == 102.15 * USD_TO_IDR_RATE
        assert df.iloc[0]['Rating'] == 3.9
        assert df.iloc[0]['Colors'] == 3
        assert df.iloc[0]['Size'] == 'M'
        assert df.iloc[0]['Gender'] == 'Women'
    
    def test_transform_data_removes_unknown_product(self):
        """Test that Unknown Product is removed."""
        sample_data = [
            {
                'Title': 'Unknown Product',
                'Price': '$100.00',
                'Rating': 'Rating: ⭐ Invalid Rating / 5',
                'Colors': '5 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Men',
                'timestamp': '2025-01-02 10:00:00'
            },
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            }
        ]
        
        df = transform_data(sample_data)
        
        assert len(df) == 1
        assert 'Unknown Product' not in df['Title'].values
    
    def test_transform_data_removes_null_values(self):
        """Test that null values are removed."""
        sample_data = [
            {
                'Title': 'Pants 16',
                'Price': 'Price Unavailable',
                'Rating': 'Rating: Not Rated',
                'Colors': '8 Colors',
                'Size': 'Size: S',
                'Gender': 'Gender: Men',
                'timestamp': '2025-01-02 10:00:00'
            },
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            }
        ]
        
        df = transform_data(sample_data)
        
        assert len(df) == 1
        assert df.iloc[0]['Title'] == 'T-shirt 2'
    
    def test_transform_data_removes_duplicates(self):
        """Test that duplicates are removed."""
        sample_data = [
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            },
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            }
        ]
        
        df = transform_data(sample_data)
        
        assert len(df) == 1
    
    def test_transform_data_correct_dtypes(self):
        """Test that data types are correct."""
        sample_data = [
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            }
        ]
        
        df = transform_data(sample_data)
        
        assert df['Title'].dtype == object  # string
        assert df['Price'].dtype == float
        assert df['Rating'].dtype == float
        assert df['Colors'].dtype in ['int32', 'int64']
        assert df['Size'].dtype == object  # string
        assert df['Gender'].dtype == object  # string
    
    def test_transform_data_empty_list(self):
        """Test transform_data with empty list."""
        with pytest.raises(ValueError) as excinfo:
            transform_data([])
        assert "non-empty list" in str(excinfo.value)
    
    def test_transform_data_none_input(self):
        """Test transform_data with None input."""
        with pytest.raises(ValueError) as excinfo:
            transform_data(None)
        assert "non-empty list" in str(excinfo.value)
    
    def test_transform_data_column_order(self):
        """Test that columns are in correct order."""
        sample_data = [
            {
                'Title': 'T-shirt 2',
                'Price': '$102.15',
                'Rating': 'Rating: ⭐ 3.9 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Women',
                'timestamp': '2025-01-02 10:00:00'
            }
        ]
        
        df = transform_data(sample_data)
        
        expected_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp']
        assert list(df.columns) == expected_columns


class TestValidateTransformedData:
    """Test cases for validate_transformed_data function."""
    
    def test_validate_success(self):
        """Test successful validation."""
        df = pd.DataFrame({
            'Title': ['T-shirt 2'],
            'Price': [1634400.0],
            'Rating': [3.9],
            'Colors': [3],
            'Size': ['M'],
            'Gender': ['Women'],
            'timestamp': ['2025-01-02 10:00:00']
        })
        
        result = validate_transformed_data(df)
        assert result is True
    
    def test_validate_empty_dataframe(self):
        """Test validation with empty DataFrame."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError) as excinfo:
            validate_transformed_data(df)
        assert "empty" in str(excinfo.value)
    
    def test_validate_none_dataframe(self):
        """Test validation with None DataFrame."""
        with pytest.raises(ValueError) as excinfo:
            validate_transformed_data(None)
        assert "empty" in str(excinfo.value)
    
    def test_validate_missing_columns(self):
        """Test validation with missing columns."""
        df = pd.DataFrame({
            'Title': ['T-shirt 2'],
            'Price': [1634400.0]
        })
        
        with pytest.raises(ValueError) as excinfo:
            validate_transformed_data(df)
        assert "Missing required columns" in str(excinfo.value)
    
    def test_validate_with_null_values(self):
        """Test validation with null values."""
        df = pd.DataFrame({
            'Title': ['T-shirt 2'],
            'Price': [None],
            'Rating': [3.9],
            'Colors': [3],
            'Size': ['M'],
            'Gender': ['Women'],
            'timestamp': ['2025-01-02 10:00:00']
        })
        
        with pytest.raises(ValueError) as excinfo:
            validate_transformed_data(df)
        assert "null values" in str(excinfo.value)
    
    def test_validate_with_unknown_product(self):
        """Test validation with Unknown Product."""
        df = pd.DataFrame({
            'Title': ['Unknown Product'],
            'Price': [1600000.0],
            'Rating': [3.9],
            'Colors': [3],
            'Size': ['M'],
            'Gender': ['Men'],
            'timestamp': ['2025-01-02 10:00:00']
        })
        
        with pytest.raises(ValueError) as excinfo:
            validate_transformed_data(df)
        assert "Unknown Product" in str(excinfo.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
