"""
Unit tests for the extract module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.extract import FashionStudioScraper, extract_data


# Sample HTML content for testing
SAMPLE_HTML = """
<!DOCTYPE html>
<html>
<body>
    <div class="collection-grid">
        <div class="collection-card">
            <div class="product-details">
                <h3 class="product-title">T-shirt 2</h3>
                <div class="price-container"><span class="price">$102.15</span></div>
                <p style="font-size: 14px; color: #777;">Rating: ⭐ 3.9 / 5</p>
                <p style="font-size: 14px; color: #777;">3 Colors</p>
                <p style="font-size: 14px; color: #777;">Size: M</p>
                <p style="font-size: 14px; color: #777;">Gender: Women</p>
            </div>
        </div>
        <div class="collection-card">
            <div class="product-details">
                <h3 class="product-title">Hoodie 3</h3>
                <div class="price-container"><span class="price">$496.88</span></div>
                <p style="font-size: 14px; color: #777;">Rating: ⭐ 4.8 / 5</p>
                <p style="font-size: 14px; color: #777;">3 Colors</p>
                <p style="font-size: 14px; color: #777;">Size: L</p>
                <p style="font-size: 14px; color: #777;">Gender: Unisex</p>
            </div>
        </div>
    </div>
</body>
</html>
"""

SAMPLE_HTML_PRICE_UNAVAILABLE = """
<!DOCTYPE html>
<html>
<body>
    <div class="collection-grid">
        <div class="collection-card">
            <div class="product-details">
                <h3 class="product-title">Pants 16</h3>
                <p class="price">Price Unavailable</p>
                <p style="font-size: 14px; color: #777;">Rating: Not Rated</p>
                <p style="font-size: 14px; color: #777;">8 Colors</p>
                <p style="font-size: 14px; color: #777;">Size: S</p>
                <p style="font-size: 14px; color: #777;">Gender: Men</p>
            </div>
        </div>
    </div>
</body>
</html>
"""

SAMPLE_HTML_UNKNOWN_PRODUCT = """
<!DOCTYPE html>
<html>
<body>
    <div class="collection-grid">
        <div class="collection-card">
            <div class="product-details">
                <h3 class="product-title">Unknown Product</h3>
                <div class="price-container"><span class="price">$100.00</span></div>
                <p style="font-size: 14px; color: #777;">Rating: ⭐ Invalid Rating / 5</p>
                <p style="font-size: 14px; color: #777;">5 Colors</p>
                <p style="font-size: 14px; color: #777;">Size: M</p>
                <p style="font-size: 14px; color: #777;">Gender: Men</p>
            </div>
        </div>
    </div>
</body>
</html>
"""

SAMPLE_HTML_NO_PRODUCTS = """
<!DOCTYPE html>
<html>
<body>
    <div class="collection-grid">
    </div>
</body>
</html>
"""


class TestFashionStudioScraper:
    """Test cases for FashionStudioScraper class."""
    
    def test_scraper_initialization(self):
        """Test that scraper initializes correctly."""
        scraper = FashionStudioScraper()
        assert scraper.BASE_URL == "https://fashion-studio.dicoding.dev/"
        assert scraper.session is not None
        scraper.close()
    
    @patch('utils.extract.requests.Session')
    def test_fetch_page_success(self, mock_session_class):
        """Test successful page fetch."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.text = SAMPLE_HTML
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        scraper = FashionStudioScraper()
        result = scraper.fetch_page(1)
        
        assert result == SAMPLE_HTML
        scraper.close()
    
    @patch('utils.extract.requests.Session')
    def test_fetch_page_second_page(self, mock_session_class):
        """Test fetching second page with correct URL."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.text = SAMPLE_HTML
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        scraper = FashionStudioScraper()
        scraper.fetch_page(2)
        
        # Check that the correct URL was called
        call_args = mock_session.get.call_args
        assert 'page2' in call_args[0][0]
        scraper.close()
    
    def test_fetch_page_invalid_page_number(self):
        """Test fetch_page with invalid page number."""
        scraper = FashionStudioScraper()
        
        with pytest.raises((ValueError, Exception)) as excinfo:
            scraper.fetch_page(0)
        assert "Invalid page number" in str(excinfo.value)
        
        with pytest.raises((ValueError, Exception)) as excinfo:
            scraper.fetch_page(51)
        assert "Invalid page number" in str(excinfo.value)
        
        with pytest.raises((ValueError, Exception)) as excinfo:
            scraper.fetch_page(-1)
        assert "Invalid page number" in str(excinfo.value)
        
        scraper.close()
    
    @patch('utils.extract.requests.Session')
    def test_fetch_page_request_error(self, mock_session_class):
        """Test fetch_page handles request errors."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.RequestException("Connection failed")
        mock_session_class.return_value = mock_session
        
        scraper = FashionStudioScraper()
        
        with pytest.raises(requests.RequestException) as excinfo:
            scraper.fetch_page(1)
        assert "Failed to fetch page" in str(excinfo.value)
        
        scraper.close()
    
    def test_parse_page_success(self):
        """Test successful page parsing."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML)
        
        assert len(products) == 2
        assert products[0]['Title'] == 'T-shirt 2'
        assert products[0]['Price'] == '$102.15'
        assert products[1]['Title'] == 'Hoodie 3'
        assert products[1]['Price'] == '$496.88'
        
        scraper.close()
    
    def test_parse_page_price_unavailable(self):
        """Test parsing page with 'Price Unavailable'."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML_PRICE_UNAVAILABLE)
        
        assert len(products) == 1
        assert products[0]['Title'] == 'Pants 16'
        assert products[0]['Price'] == 'Price Unavailable'
        
        scraper.close()
    
    def test_parse_page_unknown_product(self):
        """Test parsing page with unknown product."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML_UNKNOWN_PRODUCT)
        
        assert len(products) == 1
        assert products[0]['Title'] == 'Unknown Product'
        
        scraper.close()
    
    def test_parse_page_empty_html(self):
        """Test parse_page with empty HTML."""
        scraper = FashionStudioScraper()
        
        with pytest.raises(ValueError) as excinfo:
            scraper.parse_page("")
        assert "Invalid HTML content" in str(excinfo.value)
        
        scraper.close()
    
    def test_parse_page_no_products(self):
        """Test parse_page with no products."""
        scraper = FashionStudioScraper()
        
        with pytest.raises(ValueError) as excinfo:
            scraper.parse_page(SAMPLE_HTML_NO_PRODUCTS)
        assert "No product cards found" in str(excinfo.value)
        
        scraper.close()
    
    def test_parse_page_none_input(self):
        """Test parse_page with None input."""
        scraper = FashionStudioScraper()
        
        with pytest.raises(ValueError) as excinfo:
            scraper.parse_page(None)
        assert "Invalid HTML content" in str(excinfo.value)
        
        scraper.close()
    
    @patch.object(FashionStudioScraper, 'fetch_page')
    @patch.object(FashionStudioScraper, 'parse_page')
    def test_scrape_all_pages(self, mock_parse, mock_fetch):
        """Test scraping multiple pages."""
        mock_fetch.return_value = SAMPLE_HTML
        mock_parse.return_value = [
            {'Title': 'T-shirt 2', 'Price': '$102.15'}
        ]
        
        scraper = FashionStudioScraper()
        products = scraper.scrape_all_pages(1, 2)
        
        assert len(products) == 2
        assert all('timestamp' in p for p in products)
        
        scraper.close()
    
    def test_scrape_all_pages_invalid_range(self):
        """Test scrape_all_pages with invalid page range."""
        scraper = FashionStudioScraper()
        
        with pytest.raises((ValueError, Exception)) as excinfo:
            scraper.scrape_all_pages(5, 3)
        assert "Invalid page range" in str(excinfo.value)
        
        with pytest.raises((ValueError, Exception)) as excinfo:
            scraper.scrape_all_pages(0, 10)
        assert "Invalid page range" in str(excinfo.value)
        
        scraper.close()
    
    def test_close_session(self):
        """Test closing the session."""
        scraper = FashionStudioScraper()
        scraper.close()
        # Should not raise any errors


class TestExtractData:
    """Test cases for extract_data function."""
    
    @patch.object(FashionStudioScraper, 'scrape_all_pages')
    @patch.object(FashionStudioScraper, 'close')
    def test_extract_data_success(self, mock_close, mock_scrape):
        """Test extract_data function."""
        mock_scrape.return_value = [
            {'Title': 'T-shirt 2', 'Price': '$102.15', 'timestamp': '2025-01-02 10:00:00'}
        ]
        
        result = extract_data(1, 2)
        
        assert len(result) == 1
        assert result[0]['Title'] == 'T-shirt 2'
        mock_close.assert_called_once()
    
    @patch.object(FashionStudioScraper, 'scrape_all_pages')
    def test_extract_data_exception(self, mock_scrape):
        """Test extract_data handles exceptions."""
        mock_scrape.side_effect = Exception("Test error")
        
        with pytest.raises(Exception) as excinfo:
            extract_data(1, 2)
        assert "Extraction failed" in str(excinfo.value)


class TestProductParsing:
    """Additional tests for product parsing edge cases."""
    
    def test_parse_product_with_all_fields(self):
        """Test parsing product with all required fields."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML)
        
        product = products[0]
        assert 'Title' in product
        assert 'Price' in product
        assert 'Rating' in product
        assert 'Colors' in product
        assert 'Size' in product
        assert 'Gender' in product
        
        scraper.close()
    
    def test_parse_product_rating_format(self):
        """Test rating extraction preserves format."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML)
        
        # Check rating format
        assert '3.9' in products[0]['Rating'] or 'Rating' in products[0]['Rating']
        
        scraper.close()
    
    def test_parse_product_size_format(self):
        """Test size extraction."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML)
        
        assert 'Size:' in products[0]['Size']
        
        scraper.close()
    
    def test_parse_product_gender_format(self):
        """Test gender extraction."""
        scraper = FashionStudioScraper()
        products = scraper.parse_page(SAMPLE_HTML)
        
        assert 'Gender:' in products[0]['Gender']
        
        scraper.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
