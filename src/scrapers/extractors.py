"""
Content extraction utilities for the scraping infrastructure.

Provides HTML parsing, text extraction, link discovery, and data validation.
"""

import re
from typing import List, Dict, Optional, Any, Union
from urllib.parse import urljoin, urlparse
import hashlib

from bs4 import BeautifulSoup, Tag
from lxml import html, etree

from .exceptions import ContentExtractionError, ValidationError


class ContentExtractor:
    """Utilities for content extraction and parsing."""
    
    def __init__(self):
        self.parsers = {
            'html.parser': 'html.parser',
            'lxml': 'lxml',
            'html5lib': 'html5lib'
        }
    
    def extract_text(self, html_content: str, parser: str = 'lxml') -> str:
        """
        Extract clean text from HTML content.
        
        Args:
            html_content: Raw HTML content
            parser: Parser to use ('html.parser', 'lxml', 'html5lib')
            
        Returns:
            Clean text content
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text and clean it up
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            return text
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract text: {e}", "text")
    
    def extract_links(self, html_content: str, base_url: str, parser: str = 'lxml') -> List[str]:
        """
        Extract and validate links from HTML content.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            parser: Parser to use
            
        Returns:
            List of absolute URLs
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            links = []
            
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                
                # Skip empty links and javascript
                if not href or href.startswith('javascript:'):
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, href)
                
                # Validate URL
                if self._is_valid_url(absolute_url):
                    links.append(absolute_url)
            
            return list(set(links))  # Remove duplicates
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract links: {e}", "links")
    
    def extract_images(self, html_content: str, base_url: str, parser: str = 'lxml') -> List[str]:
        """
        Extract image URLs from HTML content.
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            parser: Parser to use
            
        Returns:
            List of image URLs
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            images = []
            
            for img in soup.find_all('img', src=True):
                src = img['src'].strip()
                
                if not src:
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, src)
                
                # Validate URL
                if self._is_valid_url(absolute_url):
                    images.append(absolute_url)
            
            return list(set(images))  # Remove duplicates
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract images: {e}", "images")
    
    def extract_forms(self, html_content: str, parser: str = 'lxml') -> List[Dict[str, Any]]:
        """
        Extract form information from HTML content.
        
        Args:
            html_content: Raw HTML content
            parser: Parser to use
            
        Returns:
            List of form data dictionaries
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            forms = []
            
            for form in soup.find_all('form'):
                form_data = {
                    'action': form.get('action', ''),
                    'method': form.get('method', 'GET'),
                    'inputs': []
                }
                
                # Extract input fields
                for input_tag in form.find_all(['input', 'textarea', 'select']):
                    # Determine the type based on tag name
                    if input_tag.name == 'textarea':
                        input_type = 'textarea'
                    elif input_tag.name == 'select':
                        input_type = 'select'
                    else:
                        input_type = input_tag.get('type', 'text')
                    
                    input_data = {
                        'type': input_type,
                        'name': input_tag.get('name', ''),
                        'value': input_tag.get('value', ''),
                        'required': input_tag.get('required') is not None
                    }
                    form_data['inputs'].append(input_data)
                
                forms.append(form_data)
            
            return forms
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract forms: {e}", "forms")
    
    def extract_metadata(self, html_content: str, parser: str = 'lxml') -> Dict[str, str]:
        """
        Extract metadata from HTML content.
        
        Args:
            html_content: Raw HTML content
            parser: Parser to use
            
        Returns:
            Dictionary of metadata
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            metadata = {}
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                metadata['title'] = title_tag.get_text().strip()
            
            # Extract meta tags
            for meta in soup.find_all('meta'):
                name = meta.get('name') or meta.get('property')
                content = meta.get('content')
                
                if name and content:
                    metadata[name] = content
            
            # Extract Open Graph tags
            for meta in soup.find_all('meta', property=re.compile(r'^og:')):
                property_name = meta.get('property', '')
                content = meta.get('content', '')
                if property_name and content:
                    metadata[property_name] = content
            
            return metadata
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract metadata: {e}", "metadata")
    
    def extract_tables(self, html_content: str, parser: str = 'lxml') -> List[List[List[str]]]:
        """
        Extract table data from HTML content.
        
        Args:
            html_content: Raw HTML content
            parser: Parser to use
            
        Returns:
            List of tables, each table is a list of rows, each row is a list of cells
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            tables = []
            
            for table in soup.find_all('table'):
                table_data = []
                
                for row in table.find_all('tr'):
                    row_data = []
                    for cell in row.find_all(['td', 'th']):
                        row_data.append(cell.get_text().strip())
                    
                    if row_data:  # Only add non-empty rows
                        table_data.append(row_data)
                
                if table_data:  # Only add non-empty tables
                    tables.append(table_data)
            
            return tables
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract tables: {e}", "tables")
    
    def validate_data(self, data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """
        Validate scraped data against a schema.
        
        Args:
            data: Scraped data dictionary
            schema: Validation schema
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            for field, rules in schema.items():
                if field not in data:
                    if rules.get('required', False):
                        raise ValidationError(f"Required field '{field}' is missing", field)
                    continue
                
                value = data[field]
                
                # Check type
                expected_type = rules.get('type')
                if expected_type and not isinstance(value, expected_type):
                    raise ValidationError(
                        f"Field '{field}' should be {expected_type.__name__}, got {type(value).__name__}",
                        field, str(value)
                    )
                
                # Check length
                if 'min_length' in rules and len(str(value)) < rules['min_length']:
                    raise ValidationError(
                        f"Field '{field}' is too short (min {rules['min_length']})",
                        field, str(value)
                    )
                
                if 'max_length' in rules and len(str(value)) > rules['max_length']:
                    raise ValidationError(
                        f"Field '{field}' is too long (max {rules['max_length']})",
                        field, str(value)
                    )
                
                # Check pattern
                if 'pattern' in rules:
                    pattern = re.compile(rules['pattern'])
                    if not pattern.match(str(value)):
                        raise ValidationError(
                            f"Field '{field}' does not match pattern {rules['pattern']}",
                            field, str(value)
                        )
            
            return True
            
        except ValidationError:
            raise
        except Exception as e:
            raise ValidationError(f"Validation error: {e}")
    
    def calculate_content_hash(self, content: str) -> str:
        """
        Calculate SHA-256 hash of content.
        
        Args:
            content: Content to hash
            
        Returns:
            SHA-256 hash string
        """
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text content.
        
        Args:
            text: Raw text content
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # Normalize unicode
        text = text.strip()
        
        return text
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def extract_structured_data(self, html_content: str, parser: str = 'lxml') -> List[Dict[str, Any]]:
        """
        Extract structured data (JSON-LD, Microdata) from HTML content.
        
        Args:
            html_content: Raw HTML content
            parser: Parser to use
            
        Returns:
            List of structured data objects
        """
        try:
            soup = BeautifulSoup(html_content, parser)
            structured_data = []
            
            # Extract JSON-LD
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    import json
                    data = json.loads(script.string)
                    structured_data.append(data)
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            # Extract Microdata
            for item in soup.find_all(attrs={'itemtype': True}):
                microdata = {
                    'type': item.get('itemtype'),
                    'properties': {}
                }
                
                for prop in item.find_all(attrs={'itemprop': True}):
                    prop_name = prop.get('itemprop')
                    prop_value = prop.get('content') or prop.get_text().strip()
                    microdata['properties'][prop_name] = prop_value
                
                structured_data.append(microdata)
            
            return structured_data
            
        except Exception as e:
            raise ContentExtractionError(f"Failed to extract structured data: {e}", "structured_data") 