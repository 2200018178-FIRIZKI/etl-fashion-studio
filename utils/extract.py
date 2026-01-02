"""
Extract module for Fashion Studio ETL Pipeline.
This module handles web scraping from https://fashion-studio.dicoding.dev/
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time


class FashionStudioScraper:
    """A class to scrape product data from Fashion Studio website."""
    
    BASE_URL = "https://fashion-studio.dicoding.dev/"
    
    def __init__(self):
        """Initialize the scraper with a session for connection pooling."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch_page(self, page_number: int) -> str:
        """
        Fetch a single page from the website.
        
        Args:
            page_number: The page number to fetch (1-50)
            
        Returns:
            HTML content of the page as string
            
        Raises:
            requests.RequestException: If the request fails
            ValueError: If page_number is invalid
        """
        try:
            if not isinstance(page_number, int) or page_number < 1 or page_number > 50:
                raise ValueError(f"Invalid page number: {page_number}. Must be between 1 and 50.")
            
            if page_number == 1:
                url = self.BASE_URL
            else:
                url = f"{self.BASE_URL}page{page_number}"
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
            
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch page {page_number}: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error fetching page {page_number}: {str(e)}")
    
    def parse_product(self, card) -> dict:
        """
        Parse a single product card and extract data.
        
        Args:
            card: BeautifulSoup element representing a product card
            
        Returns:
            Dictionary containing product data
            
        Raises:
            ValueError: If required data cannot be extracted
        """
        try:
            product = {}
            
            # Extract Title
            title_elem = card.find('h3', class_='product-title')
            if title_elem:
                product['Title'] = title_elem.get_text(strip=True)
            else:
                raise ValueError("Could not find product title")
            
            # Extract Price (handle both formats)
            price_container = card.find('div', class_='price-container')
            if price_container:
                price_elem = price_container.find('span', class_='price')
                product['Price'] = price_elem.get_text(strip=True) if price_elem else None
            else:
                # Alternative format for "Price Unavailable"
                price_elem = card.find('p', class_='price')
                product['Price'] = price_elem.get_text(strip=True) if price_elem else None
            
            # Extract all paragraph elements with product details
            details = card.find_all('p', style=lambda x: x and '777' in x)
            
            for detail in details:
                text = detail.get_text(strip=True)
                if 'Rating:' in text:
                    product['Rating'] = text
                elif 'Colors' in text:
                    product['Colors'] = text
                elif 'Size:' in text:
                    product['Size'] = text
                elif 'Gender:' in text:
                    product['Gender'] = text
            
            # Handle "Rating: Not Rated" case (different html structure)
            if 'Rating' not in product:
                for detail in details:
                    text = detail.get_text(strip=True)
                    if 'Rated' in text or 'Rating' in text:
                        product['Rating'] = text
            
            return product
            
        except Exception as e:
            raise ValueError(f"Error parsing product card: {str(e)}")
    
    def parse_page(self, html_content: str) -> list:
        """
        Parse all products from a page's HTML content.
        
        Args:
            html_content: HTML string of the page
            
        Returns:
            List of product dictionaries
            
        Raises:
            ValueError: If parsing fails
        """
        try:
            if not html_content or not isinstance(html_content, str):
                raise ValueError("Invalid HTML content provided")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            product_cards = soup.find_all('div', class_='collection-card')
            
            if not product_cards:
                raise ValueError("No product cards found on the page")
            
            products = []
            for card in product_cards:
                try:
                    product = self.parse_product(card)
                    products.append(product)
                except ValueError as e:
                    # Log error but continue processing other products
                    print(f"Warning: Skipping product due to parse error: {e}")
                    continue
            
            return products
            
        except Exception as e:
            raise ValueError(f"Error parsing page: {str(e)}")
    
    def scrape_all_pages(self, start_page: int = 1, end_page: int = 50) -> list:
        """
        Scrape all products from multiple pages.
        
        Args:
            start_page: First page to scrape (default: 1)
            end_page: Last page to scrape (default: 50)
            
        Returns:
            List of all product dictionaries with timestamp
            
        Raises:
            ValueError: If page range is invalid
            requests.RequestException: If fetching fails
        """
        try:
            if start_page < 1 or end_page > 50 or start_page > end_page:
                raise ValueError(f"Invalid page range: {start_page} to {end_page}")
            
            all_products = []
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for page_num in range(start_page, end_page + 1):
                print(f"Scraping page {page_num}/{end_page}...")
                
                try:
                    html_content = self.fetch_page(page_num)
                    products = self.parse_page(html_content)
                    
                    # Add timestamp to each product
                    for product in products:
                        product['timestamp'] = timestamp
                    
                    all_products.extend(products)
                    
                    # Be polite to the server
                    if page_num < end_page:
                        time.sleep(0.5)
                        
                except Exception as e:
                    print(f"Error on page {page_num}: {e}")
                    continue
            
            print(f"Successfully scraped {len(all_products)} products from {end_page - start_page + 1} pages")
            return all_products
            
        except Exception as e:
            raise Exception(f"Error during scraping: {str(e)}")
    
    def close(self):
        """Close the session."""
        try:
            self.session.close()
        except Exception as e:
            raise Exception(f"Error closing session: {str(e)}")


def extract_data(start_page: int = 1, end_page: int = 50) -> list:
    """
    Main extraction function to scrape all product data.
    
    Args:
        start_page: First page to scrape (default: 1)
        end_page: Last page to scrape (default: 50)
        
    Returns:
        List of product dictionaries
        
    Raises:
        Exception: If extraction fails
    """
    try:
        scraper = FashionStudioScraper()
        products = scraper.scrape_all_pages(start_page, end_page)
        scraper.close()
        return products
    except Exception as e:
        raise Exception(f"Extraction failed: {str(e)}")


if __name__ == "__main__":
    # Test the extraction
    products = extract_data(1, 2)  # Test with first 2 pages
    for product in products[:5]:
        print(product)
