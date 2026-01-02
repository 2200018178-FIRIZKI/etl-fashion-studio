"""
Transform module for Fashion Studio ETL Pipeline.
This module handles data cleaning and transformation.
"""

import pandas as pd
import re


# Exchange rate: 1 USD = 16,000 IDR
USD_TO_IDR_RATE = 16000


def clean_price(price_str: str) -> float:
    """
    Clean and convert price from USD string to IDR float.
    
    Args:
        price_str: Price string (e.g., "$102.15" or "Price Unavailable")
        
    Returns:
        Price in IDR as float, or None if invalid
        
    Raises:
        ValueError: If price string format is unexpected
    """
    try:
        if not price_str or not isinstance(price_str, str):
            return None
        
        price_str = price_str.strip()
        
        # Check for invalid price
        if 'Unavailable' in price_str or 'unavailable' in price_str:
            return None
        
        # Extract numeric value from price string (e.g., "$102.15" -> 102.15)
        match = re.search(r'\$?([\d,]+\.?\d*)', price_str)
        if match:
            price_usd = float(match.group(1).replace(',', ''))
            price_idr = price_usd * USD_TO_IDR_RATE
            return price_idr
        
        return None
        
    except Exception as e:
        raise ValueError(f"Error cleaning price '{price_str}': {str(e)}")


def clean_rating(rating_str: str) -> float:
    """
    Clean rating string and extract numeric value.
    
    Args:
        rating_str: Rating string (e.g., "Rating: ⭐ 3.9 / 5" or "Rating: ⭐ Invalid Rating / 5")
        
    Returns:
        Rating as float, or None if invalid
        
    Raises:
        ValueError: If rating string format is unexpected
    """
    try:
        if not rating_str or not isinstance(rating_str, str):
            return None
        
        rating_str = rating_str.strip()
        
        # Check for invalid ratings
        if 'Invalid' in rating_str or 'Not Rated' in rating_str:
            return None
        
        # Extract numeric rating (e.g., "Rating: ⭐ 3.9 / 5" -> 3.9)
        match = re.search(r'(\d+\.?\d*)\s*/\s*5', rating_str)
        if match:
            return float(match.group(1))
        
        # Alternative: just a number
        match = re.search(r'(\d+\.?\d*)', rating_str)
        if match:
            return float(match.group(1))
        
        return None
        
    except Exception as e:
        raise ValueError(f"Error cleaning rating '{rating_str}': {str(e)}")


def clean_colors(colors_str: str) -> int:
    """
    Clean colors string and extract numeric value.
    
    Args:
        colors_str: Colors string (e.g., "3 Colors")
        
    Returns:
        Number of colors as integer
        
    Raises:
        ValueError: If colors string format is unexpected
    """
    try:
        if not colors_str or not isinstance(colors_str, str):
            return None
        
        colors_str = colors_str.strip()
        
        # Extract numeric value (e.g., "3 Colors" -> 3)
        match = re.search(r'(\d+)', colors_str)
        if match:
            return int(match.group(1))
        
        return None
        
    except Exception as e:
        raise ValueError(f"Error cleaning colors '{colors_str}': {str(e)}")


def clean_size(size_str: str) -> str:
    """
    Clean size string and extract size value.
    
    Args:
        size_str: Size string (e.g., "Size: M")
        
    Returns:
        Size as string (e.g., "M")
        
    Raises:
        ValueError: If size string format is unexpected
    """
    try:
        if not size_str or not isinstance(size_str, str):
            return None
        
        size_str = size_str.strip()
        
        # Remove "Size: " prefix
        size = size_str.replace('Size:', '').strip()
        
        if size:
            return size
        
        return None
        
    except Exception as e:
        raise ValueError(f"Error cleaning size '{size_str}': {str(e)}")


def clean_gender(gender_str: str) -> str:
    """
    Clean gender string and extract gender value.
    
    Args:
        gender_str: Gender string (e.g., "Gender: Men")
        
    Returns:
        Gender as string (e.g., "Men")
        
    Raises:
        ValueError: If gender string format is unexpected
    """
    try:
        if not gender_str or not isinstance(gender_str, str):
            return None
        
        gender_str = gender_str.strip()
        
        # Remove "Gender: " prefix
        gender = gender_str.replace('Gender:', '').strip()
        
        if gender:
            return gender
        
        return None
        
    except Exception as e:
        raise ValueError(f"Error cleaning gender '{gender_str}': {str(e)}")


def transform_data(data: list) -> pd.DataFrame:
    """
    Transform raw scraped data into a clean DataFrame.
    
    Args:
        data: List of product dictionaries from extraction
        
    Returns:
        Cleaned pandas DataFrame
        
    Raises:
        ValueError: If data is invalid or empty
        Exception: If transformation fails
    """
    try:
        if not data or not isinstance(data, list):
            raise ValueError("Input data must be a non-empty list")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        print(f"Initial data count: {len(df)}")
        
        # Apply cleaning functions
        df['Price'] = df['Price'].apply(clean_price)
        df['Rating'] = df['Rating'].apply(clean_rating)
        df['Colors'] = df['Colors'].apply(clean_colors)
        df['Size'] = df['Size'].apply(clean_size)
        df['Gender'] = df['Gender'].apply(clean_gender)
        
        # Remove invalid products (Unknown Product)
        df = df[df['Title'] != 'Unknown Product']
        print(f"After removing 'Unknown Product': {len(df)}")
        
        # Remove rows with null values
        df = df.dropna()
        print(f"After removing null values: {len(df)}")
        
        # Remove duplicates
        df = df.drop_duplicates()
        print(f"After removing duplicates: {len(df)}")
        
        # Ensure correct data types
        df['Title'] = df['Title'].astype(str)
        df['Price'] = df['Price'].astype(float)
        df['Rating'] = df['Rating'].astype(float)
        df['Colors'] = df['Colors'].astype(int)
        df['Size'] = df['Size'].astype(str)
        df['Gender'] = df['Gender'].astype(str)
        
        # Reorder columns
        columns_order = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp']
        df = df[columns_order]
        
        # Reset index
        df = df.reset_index(drop=True)
        
        print(f"Final clean data count: {len(df)}")
        
        return df
        
    except ValueError as e:
        raise ValueError(f"Validation error during transformation: {str(e)}")
    except Exception as e:
        raise Exception(f"Transformation failed: {str(e)}")


def validate_transformed_data(df: pd.DataFrame) -> bool:
    """
    Validate that the transformed data meets all requirements.
    
    Args:
        df: Transformed DataFrame to validate
        
    Returns:
        True if validation passes
        
    Raises:
        ValueError: If validation fails
    """
    try:
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        # Check for required columns
        required_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp']
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            raise ValueError(f"DataFrame contains null values: {null_counts[null_counts > 0].to_dict()}")
        
        # Check for duplicates
        if df.duplicated().any():
            raise ValueError("DataFrame contains duplicate rows")
        
        # Check for invalid products
        if 'Unknown Product' in df['Title'].values:
            raise ValueError("DataFrame contains 'Unknown Product' entries")
        
        # Check data types
        if df['Price'].dtype != float:
            raise ValueError(f"Price column should be float, got {df['Price'].dtype}")
        
        if df['Rating'].dtype != float:
            raise ValueError(f"Rating column should be float, got {df['Rating'].dtype}")
        
        if df['Colors'].dtype not in [int, 'int64', 'int32']:
            raise ValueError(f"Colors column should be int, got {df['Colors'].dtype}")
        
        print("Data validation passed!")
        return True
        
    except Exception as e:
        raise ValueError(f"Data validation failed: {str(e)}")


if __name__ == "__main__":
    # Test transformation with sample data
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
            'Title': 'Unknown Product',
            'Price': '$100.00',
            'Rating': 'Rating: ⭐ Invalid Rating / 5',
            'Colors': '5 Colors',
            'Size': 'Size: M',
            'Gender': 'Gender: Men',
            'timestamp': '2025-01-02 10:00:00'
        }
    ]
    
    df = transform_data(sample_data)
    print(df)
    print(df.dtypes)
