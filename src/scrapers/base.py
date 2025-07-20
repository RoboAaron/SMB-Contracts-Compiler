"""
Base scraper class for the Texas WBE Opportunity Discovery Engine.

Provides common functionality for all portal-specific scrapers including
session management, rate limiting, error handling, and data extraction.
"""

import asyncio
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from decimal import Decimal

from ..config import Config
from ..database.models import Opportunity, ScrapingLog
from ..database.connection import get_db_session
from .http_client import HTTPClientManager
from .rate_limiter import RateLimiter
from .extractors import ContentExtractor
from .exceptions import ScrapingError, RateLimitError, RobotsTxtError, ValidationError


class BaseScraper(ABC):
    """Abstract base class for all scrapers."""
    
    def __init__(self, config: Union[Config, Dict[str, Any]], portal_name: str):
        self.config = config
        self.portal_name = portal_name
        self.logger = logging.getLogger(f"{__name__}.{portal_name}")
        self.http_client: Optional[HTTPClientManager] = None
        self.rate_limiter: Optional[RateLimiter] = None
        self.extractor: Optional[ContentExtractor] = None
        self.session = None
        self.stats = {
            'requests_made': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'opportunities_found': 0,
            'start_time': None,
            'end_time': None
        }
    
    async def initialize(self):
        """Initialize scraper resources."""
        try:
            # Initialize HTTP client manager
            self.http_client = HTTPClientManager(self.config)
            await self.http_client.initialize()
            
            # Initialize rate limiter
            self.rate_limiter = RateLimiter(self.config)
            
            # Initialize content extractor
            self.extractor = ContentExtractor()
            
            # Initialize database session
            self.session = get_db_session()
            
            # Set start time
            self.stats['start_time'] = datetime.now()
            
        except Exception as e:
            raise ScrapingError(f"Failed to initialize scraper: {e}")
    
    @abstractmethod
    async def scrape_opportunities(self) -> List[Opportunity]:
        """
        Main scraping method to be implemented by subclasses.
        
        Returns:
            List of scraped opportunities
        """
        pass
    
    async def make_request(self, url: str, **kwargs) -> Any:
        """
        Make HTTP request with rate limiting and logging.
        
        Args:
            url: URL to request
            **kwargs: Additional request parameters
            
        Returns:
            Response object or content
        """
        start_time = time.time()
        
        try:
            # Check robots.txt
            if not await self.rate_limiter.check_robots_txt(url):
                raise RobotsTxtError(f"URL not allowed by robots.txt: {url}", url)
            
            # Apply rate limiting
            delay = await self.rate_limiter.respect_rate_limit(url)
            
            # Make request
            response = await self.http_client.make_request(url, **kwargs)
            
            # Calculate response time
            response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # Log successful request
            await self._log_request(
                url=url,
                status_code=getattr(response, 'status', 200),
                response_time=response_time,
                success=True
            )
            
            self.stats['requests_made'] += 1
            self.stats['successful_requests'] += 1
            
            return response
            
        except Exception as e:
            # Calculate response time
            response_time = (time.time() - start_time) * 1000
            
            # Log failed request
            await self._log_request(
                url=url,
                status_code=getattr(e, 'status_code', 0),
                response_time=response_time,
                success=False,
                error_message=str(e)
            )
            
            self.stats['requests_made'] += 1
            self.stats['failed_requests'] += 1
            
            raise
    
    async def _log_request(
        self, 
        url: str, 
        status_code: int, 
        response_time: float, 
        success: bool, 
        error_message: str = None
    ):
        """Log scraping request to database."""
        try:
            # Handle both Config object and dict
            if hasattr(self.config, 'scraping'):
                user_agent = self.config.scraping.user_agent
                robots_txt_respected = self.config.scraping.respect_robots_txt
                rate_limit_delay = self.config.scraping.request_delay
            else:
                # Fallback for dict config
                user_agent = self.config.get('user_agent', 'OpportunityEngine/1.0')
                robots_txt_respected = self.config.get('respect_robots_txt', True)
                rate_limit_delay = self.config.get('request_delay', 3.0)
            
            log_entry = ScrapingLog(
                portal_name=self.portal_name,
                url_scraped=url,
                status_code=status_code,
                response_time_ms=response_time,
                success=success,
                error_message=error_message,
                user_agent=user_agent,
                robots_txt_respected=robots_txt_respected,
                rate_limit_delay=rate_limit_delay
            )
            
            self.session.add(log_entry)
            await self.session.commit()
            
        except Exception as e:
            # Don't let logging errors break the scraping process
            print(f"Warning: Failed to log request: {e}")
    
    def extract_text(self, html_content: str) -> str:
        """Extract clean text from HTML content."""
        return self.extractor.extract_text(html_content)
    
    def extract_links(self, html_content: str, base_url: str) -> List[str]:
        """Extract links from HTML content."""
        return self.extractor.extract_links(html_content, base_url)
    
    def extract_metadata(self, html_content: str) -> Dict[str, str]:
        """Extract metadata from HTML content."""
        return self.extractor.extract_metadata(html_content)
    
    def validate_opportunity_data(self, data: Dict[str, Any]) -> bool:
        """Validate opportunity data against schema."""
        schema = {
            'external_id': {'type': str, 'required': True, 'min_length': 1},
            'source_portal': {'type': str, 'required': True, 'min_length': 1},
            'title': {'type': str, 'required': True, 'min_length': 1},
            'description_short': {'type': str, 'required': False},
            'status': {'type': str, 'required': False},
            'post_date': {'type': datetime, 'required': False},
            'due_date': {'type': datetime, 'required': False},
            'opportunity_url': {'type': str, 'required': False},
            'estimated_value': {'type': (int, float), 'required': False}
        }
        
        return self.extractor.validate_data(data, schema)
    
    def create_opportunity(self, data: Dict[str, Any]) -> Opportunity:
        """Create Opportunity object from scraped data."""
        # Validate data
        if not self.validate_opportunity_data(data):
            raise ValidationError("Invalid opportunity data")
        
        # Set default values
        data.setdefault('source_portal', self.portal_name)
        data.setdefault('status', 'Open')
        
        return Opportunity(**data)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        stats = self.stats.copy()
        
        if stats['start_time']:
            if stats['end_time']:
                duration = (stats['end_time'] - stats['start_time']).total_seconds()
            else:
                duration = (datetime.now() - stats['start_time']).total_seconds()
            
            stats['duration_seconds'] = duration
            stats['requests_per_second'] = stats['requests_made'] / duration if duration > 0 else 0
            stats['success_rate'] = stats['successful_requests'] / stats['requests_made'] if stats['requests_made'] > 0 else 0
        
        return stats
    
    async def cleanup(self):
        """Clean up scraper resources."""
        try:
            # Set end time
            self.stats['end_time'] = datetime.now()
            
            # Clean up HTTP client
            if self.http_client:
                await self.http_client.cleanup()
            
            # Close database session
            if self.session:
                await self.session.close()
                
        except Exception as e:
            print(f"Warning: Error during scraper cleanup: {e}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()
    
    def __repr__(self) -> str:
        """String representation."""
        return f"<{self.__class__.__name__}(portal='{self.portal_name}')>"
    
    def extract_standardized_opportunity_data(self, element, header_info: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Extract opportunity data using standardized labels.
        
        This method provides a consistent way to extract data across all portals
        using standardized field names that map to the Opportunity model.
        """
        extracted_data = {}
        try:
            # Standard field mappings for all portals
            standard_mappings = {
                'bid_number': ['bid number', 'reference number', 'solicitation number', 'opportunity id', 'contract number'],
                'title': ['title', 'description', 'project name', 'solicitation title'],
                'issuing_entity_name': ['agency', 'entity', 'department', 'organization', 'government entity'],
                'issuing_entity_id': ['entity id', 'agency id', 'department code'],
                'post_date': ['release date', 'post date', 'published date', 'issue date'],
                'due_date': ['due date', 'deadline', 'closing date', 'submission deadline', 'bid deadline'],
                'status': ['status', 'state', 'opportunity status'],
                'estimated_value': ['estimated value', 'value', 'amount', 'contract value', 'budget'],
                'contact_info': ['contact', 'contact information', 'contact person', 'point of contact'],
                'wbe_requirements': ['wbe', 'wbe requirements', 'minority', 'disadvantaged', 'diversity requirements'],
                'nigp_codes': ['nigp', 'nigp codes', 'commodity codes', 'classification codes'],
                'days_remaining': ['days left', 'remaining days', 'time remaining']
            }
            # If header_info is a tag, convert to mapping; if string, set to None
            from bs4 import Tag
            if header_info and not isinstance(header_info, dict):
                if hasattr(header_info, 'get_text'):
                    header_info = self.extract_header_mappings(header_info)
                else:
                    header_info = None
            # Extract data using header mappings if available
            if header_info:
                extracted_data.update(self._extract_by_header_mapping(element, header_info))
            # Fallback to pattern-based extraction
            element_text = element.get_text(strip=True).lower()
            for standard_field, portal_patterns in standard_mappings.items():
                if standard_field not in extracted_data:
                    for pattern in portal_patterns:
                        if pattern in element_text:
                            value = self._extract_field_value(element, pattern)
                            if value:
                                extracted_data[standard_field] = value
                                break
            # Add source tracking
            extracted_data['source_portal'] = self.portal_name
            extracted_data['extraction_timestamp'] = datetime.now().isoformat()
        except Exception as e:
            self.logger.error(f"Error extracting standardized data: {e}")
        return extracted_data
    
    def _extract_by_header_mapping(self, element, header_info: Dict[str, str]) -> Dict[str, Any]:
        """Extract data using header column mappings."""
        extracted_data = {}
        
        try:
            # For table rows, extract by column position
            if element.name == 'tr':
                cells = element.find_all(['td', 'th'])
                
                # Map column positions to standardized fields based on header
                for standard_label, portal_label in header_info.items():
                    # Find the column index for this field
                    column_index = self._find_column_index(element.parent, portal_label)
                    if column_index is not None and column_index < len(cells):
                        cell_text = cells[column_index].get_text(strip=True)
                        
                        # Parse based on field type
                        if standard_label in ['post_date', 'due_date']:
                            extracted_data[standard_label] = self._parse_date(cell_text)
                        elif standard_label == 'estimated_value':
                            extracted_data[standard_label] = self._parse_currency(cell_text)
                        else:
                            extracted_data[standard_label] = cell_text
            
        except Exception as e:
            self.logger.error(f"Error extracting by header mapping: {e}")
        
        return extracted_data
    
    def _find_column_index(self, table_element, portal_label: str) -> Optional[int]:
        """Find the column index for a given portal label."""
        try:
            # Look for header row
            header_row = table_element.find('tr')
            if not header_row:
                return None
            
            cells = header_row.find_all(['th', 'td'])
            for i, cell in enumerate(cells):
                cell_text = cell.get_text(strip=True).lower()
                if portal_label.lower() in cell_text:
                    return i
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding column index: {e}")
            return None
    
    def _extract_field_value(self, element, field_pattern: str) -> Optional[str]:
        """Extract the value associated with a field pattern."""
        try:
            # Look for the pattern in the element or its children
            element_text = element.get_text()
            
            # Simple pattern matching - can be enhanced with regex
            if field_pattern in element_text.lower():
                # Try to extract the value that follows the pattern
                # This is a basic implementation - can be improved
                return element_text.strip()
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting field value: {e}")
            return None
    
    def _parse_currency(self, text: str) -> Optional[Decimal]:
        """Parse currency values from text."""
        if not text:
            return None
        
        try:
            import re
            from decimal import Decimal
            
            # Remove common currency symbols and formatting
            cleaned = re.sub(r'[$,€£¥]', '', text.strip())
            cleaned = re.sub(r'[,\s]', '', cleaned)
            
            # Try to extract numeric value
            match = re.search(r'(\d+(?:\.\d{2})?)', cleaned)
            if match:
                return Decimal(match.group(1))
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error parsing currency: {e}")
            return None
    
    def detect_header_row(self, element) -> bool:
        """Detect if an element is a header row that should be skipped."""
        try:
            text = element.get_text(strip=True).lower()
            
            # Common header indicators
            header_indicators = [
                'status', 'reference', 'title', 'release', 'due', 'deadline',
                'agency', 'entity', 'contact', 'nigp', 'wbe', 'value', 'amount',
                'bid number', 'solicitation', 'opportunity'
            ]
            
            # Count header indicators
            indicator_count = sum(1 for indicator in header_indicators if indicator in text)
            
            # Check for table header styling
            has_header_styling = (
                element.get('class') and any('header' in cls.lower() for cls in element.get('class', [])) or
                element.get('role') == 'columnheader' or
                element.find('th') is not None
            )
            
            # Check if it's the first row
            is_first_row = element == element.parent.find('tr') if element.name == 'tr' else False
            
            return (indicator_count >= 3) or (is_first_row and indicator_count >= 2) or has_header_styling
            
        except Exception as e:
            self.logger.error(f"Error detecting header row: {e}")
            return False
    
    def extract_header_mappings(self, header_element) -> Dict[str, str]:
        """Extract header information to map portal-specific labels to standardized labels."""
        header_info = {}
        
        try:
            header_text = header_element.get_text(strip=True).lower()
            
            # Comprehensive mapping of portal labels to standardized labels
            header_mappings = {
                # Core identification
                'status': 'status',
                'reference number': 'bid_number',
                'bid number': 'bid_number',
                'solicitation number': 'bid_number',
                'opportunity id': 'bid_number',
                'contract number': 'bid_number',
                'title': 'title',
                'description': 'title',
                'project name': 'title',
                'solicitation title': 'title',
                
                # Dates
                'release date': 'post_date',
                'post date': 'post_date',
                'published date': 'post_date',
                'issue date': 'post_date',
                'due date': 'due_date',
                'deadline': 'due_date',
                'closing date': 'due_date',
                'submission deadline': 'due_date',
                'bid deadline': 'due_date',
                
                # Entity information
                'agency': 'issuing_entity_name',
                'entity': 'issuing_entity_name',
                'department': 'issuing_entity_name',
                'organization': 'issuing_entity_name',
                'government entity': 'issuing_entity_name',
                
                # Contact and requirements
                'contact': 'contact_info',
                'contact information': 'contact_info',
                'contact person': 'contact_info',
                'point of contact': 'contact_info',
                'nigp': 'nigp_codes',
                'nigp codes': 'nigp_codes',
                'commodity codes': 'nigp_codes',
                'classification codes': 'nigp_codes',
                'wbe': 'wbe_requirements',
                'wbe requirements': 'wbe_requirements',
                'minority': 'wbe_requirements',
                'disadvantaged': 'wbe_requirements',
                'diversity requirements': 'wbe_requirements',
                
                # Values and timing
                'estimated value': 'estimated_value',
                'value': 'estimated_value',
                'amount': 'estimated_value',
                'contract value': 'estimated_value',
                'budget': 'estimated_value',
                'days left': 'days_remaining',
                'remaining days': 'days_remaining',
                'time remaining': 'days_remaining'
            }
            
            # Find which standardized labels are present in the header
            for portal_label, standard_label in header_mappings.items():
                if portal_label in header_text:
                    header_info[standard_label] = portal_label
            
            self.logger.info(f"Detected header mappings: {header_info}")
            
        except Exception as e:
            self.logger.error(f"Error extracting header mappings: {e}")
        
        return header_info 