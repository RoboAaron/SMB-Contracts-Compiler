"""
ESBD (Electronic State Business Daily) Portal Scraper

This module implements a specialized scraper for the Texas Electronic State Business Daily
portal to extract procurement opportunities where WBE status provides competitive advantages.

Updated to work with the new TXSmartBuy portal (JavaScript-heavy application).
"""

import re
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass

from bs4 import BeautifulSoup
import requests

from .enhanced_base import EnhancedBaseScraper
from .exceptions import ScrapingError, ParsingError, DocumentDownloadError
from .extractors import ContentExtractor
from ..database.models import Opportunity, Document, NIGPCode, ScrapingLog
from ..database.connection import get_db
from ..config import Config
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Try to import Selenium components
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium not available. ESBD scraper will use fallback methods.")


class DictAccessWrapper:
    """Wrapper to provide dict-style access to configuration objects."""
    
    def __init__(self, config_obj):
        self.config_obj = config_obj
    
    def __getitem__(self, key):
        """Allow dict-style access."""
        if isinstance(self.config_obj, dict):
            return self.config_obj[key]
        else:
            return getattr(self.config_obj, key)
    
    def get(self, key, default=None):
        """Get method for dict-style access."""
        try:
            return self[key]
        except (KeyError, AttributeError):
            return default
    
    def __getattr__(self, name):
        """Delegate attribute access to wrapped object."""
        return getattr(self.config_obj, name)


@dataclass
class ESBDOpportunity:
    """Data class for ESBD opportunity data before normalization."""
    title: str
    description: str
    agency: str
    bid_number: str
    submission_deadline: Optional[datetime]
    pre_bid_meeting: Optional[datetime]
    contact_name: Optional[str]
    contact_email: Optional[str]
    contact_phone: Optional[str]
    nigp_codes: List[str]
    wbe_requirements: List[str]
    document_urls: List[str]
    portal_url: str
    raw_data: Dict[str, Any]


class ESBDScraper(EnhancedBaseScraper):
    """
    ESBD Portal Scraper
    
    Implements specialized scraping logic for the Texas Electronic State Business Daily
    portal, focusing on opportunities where WBE status provides competitive advantages.
    
    Updated to work with the new TXSmartBuy portal that uses JavaScript-heavy dynamic content.
    """
    
    def __init__(self, config: Config):
        """Initialize ESBD scraper with configuration."""
        # Get ESBD-specific configuration from scrapers section
        if hasattr(config, 'scrapers') and 'esbd' in config.scrapers:
            portal_config = config.scrapers['esbd']
        elif hasattr(config, 'portals') and 'esbd' in config.portals:
            # Fallback to portals section for backward compatibility
            portal_config = config.portals['esbd']
        else:
            # Create default configuration if none found
            portal_config = {}
        
        # Get base URL from configuration
        if hasattr(portal_config, 'base_url'):
            base_url = portal_config.base_url
        elif isinstance(portal_config, dict) and 'base_url' in portal_config:
            base_url = portal_config['base_url']
        else:
            base_url = 'http://www.txsmartbuy.gov/esbd'
        
        # ESBD-specific configuration (MOVED BEFORE super().__init__)
        if hasattr(portal_config, 'search_url'):
            self.search_url = portal_config.search_url
        elif isinstance(portal_config, dict) and 'search_url' in portal_config:
            self.search_url = portal_config['search_url']
        else:
            self.search_url = base_url
            
        if hasattr(portal_config, 'detail_url'):
            self.detail_url = portal_config.detail_url
        elif isinstance(portal_config, dict) and 'detail_url' in portal_config:
            self.detail_url = portal_config['detail_url']
        else:
            self.detail_url = base_url
            
        if hasattr(portal_config, 'document_base_url'):
            self.document_base_url = portal_config.document_base_url
        elif isinstance(portal_config, dict) and 'document_base_url' in portal_config:
            self.document_base_url = portal_config['document_base_url']
        else:
            self.document_base_url = base_url
        
        # Check if Selenium is required and available
        self.requires_selenium = getattr(portal_config, 'requires_selenium', False) or \
                                (isinstance(portal_config, dict) and portal_config.get('requires_selenium', False))
        
        if self.requires_selenium and not SELENIUM_AVAILABLE:
            logger.warning("Selenium is required but not available. Falling back to basic scraping.")
            self.requires_selenium = False
        
        # Initialize enhanced base scraper with portal config (disable AI analysis by default)
        super().__init__(portal_config, "ESBD", enable_ai_analysis=False)
        
        # Store configuration for tests - make it dict-accessible
        self.base_url = base_url
        self.portal_config = DictAccessWrapper(portal_config)
        
        self.extractor = ContentExtractor()
        
        # Initialize HTTP client for sync operations
        self.http_client = requests.Session()
        self.http_client.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(config.scraping)
        
        # Initialize Selenium driver if required
        self.driver = None
        if self.requires_selenium:
            self._init_selenium_driver()
        
        logger.info(f"ESBD Scraper initialized for {base_url} (Selenium: {self.requires_selenium})")
    
    def _init_selenium_driver(self):
        """Initialize Selenium WebDriver for JavaScript-heavy content."""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logger.info("Selenium WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Selenium WebDriver: {e}")
            self.requires_selenium = False
            self.driver = None
    
    def __del__(self):
        """Clean up Selenium driver on destruction."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
    
    @property
    def selectors(self) -> Dict[str, str]:
        """Get selectors from portal configuration."""
        if hasattr(self.portal_config, 'selectors'):
            return self.portal_config.selectors
        elif hasattr(self.portal_config, 'get'):
            return self.portal_config.get('selectors', {})
        else:
            return {}
    
    @property
    def portal_config_dict(self):
        """Return portal_config as a dict for test compatibility."""
        if hasattr(self.portal_config, 'model_dump'):
            return self.portal_config.model_dump()
        elif isinstance(self.portal_config, dict):
            return self.portal_config
        else:
            return dict(self.portal_config)

    def scrape_opportunities(self, max_pages: int = 10, days_back: int = None, start_date: datetime = None, end_date: datetime = None) -> List[Opportunity]:
        """
        Scrape opportunities from ESBD portal using enhanced architecture.
        
        Args:
            max_pages: Maximum number of pages to scrape
            days_back: Number of days back to scrape (e.g., 7 for last week)
            start_date: Specific start date for filtering
            end_date: Specific end date for filtering
            
        Returns:
            List of Opportunity objects
        """
        logger.info(f"Starting ESBD opportunity scraping (max pages: {max_pages}, days_back: {days_back}, start_date: {start_date}, end_date: {end_date})")
        
        # Calculate date range if days_back is specified
        if days_back:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        opportunities = []
        
        try:
            # Try Selenium first if enabled
            if self.requires_selenium and self.driver:
                try:
                    logger.info("Attempting to scrape with Selenium...")
                    opportunities = self._scrape_with_selenium(max_pages, start_date, end_date)
                    if opportunities:
                        logger.info(f"Selenium scraping successful: {len(opportunities)} opportunities")
                        self.log_scraping_activity(len(opportunities))
                        return opportunities
                except Exception as e:
                    logger.warning(f"Selenium scraping failed: {e}")
                    logger.info("Falling back to requests method...")
            
            # Try requests method
            try:
                logger.info("Attempting to scrape with requests...")
                opportunities = self._scrape_with_requests(max_pages, start_date, end_date)
                if opportunities:
                    logger.info(f"Requests scraping successful: {len(opportunities)} opportunities")
                    self.log_scraping_activity(len(opportunities))
                    return opportunities
            except Exception as e:
                logger.warning(f"Requests scraping failed: {e}")
            
            # If both methods fail, return empty list with warning
            logger.warning("Both Selenium and requests scraping failed. ESBD portal may be temporarily unavailable.")
            logger.info("ESBD scraping completed. Found 0 opportunities (portal unavailable)")
            self.log_scraping_activity(0, errors=1)
            return []
            
        except Exception as e:
            logger.error(f"Unexpected error during ESBD scraping: {e}")
            self.log_scraping_activity(0, errors=1)
            return []
    
    def _scrape_with_selenium(self, max_pages: int, start_date: datetime, end_date: datetime) -> List[Opportunity]:
        """Scrape opportunities using Selenium for JavaScript-heavy content."""
        if not self.driver:
            raise ScrapingError("Selenium driver not initialized")
        
        opportunities = []
        
        try:
            # Navigate to the ESBD portal
            logger.info(f"Navigating to ESBD portal: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Wait for page to load
            time.sleep(5)
            
            # Wait for content to be loaded dynamically
            try:
                # Wait for body to be present
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Wait for ESBD-specific content to load
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "esbd-result-row"))
                )
                
                logger.info("ESBD content loaded successfully")
                
            except TimeoutException:
                logger.warning("Timeout waiting for ESBD content to load completely")
                # Continue anyway, the content might still be there
            
            # Additional wait to ensure all content is rendered
            time.sleep(3)
            
            # Get the page source after JavaScript has executed
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Look for opportunities in the dynamic content
            page_opportunities = self._extract_opportunities_from_dynamic_content(soup, start_date, end_date)
            
            # Process each opportunity
            for opp_data in page_opportunities:
                try:
                    opportunity = self._process_opportunity(opp_data)
                    if opportunity:
                        opportunities.append(opportunity)
                except Exception as e:
                    logger.error(f"Error processing opportunity: {e}")
                    continue
            
            logger.info(f"Extracted {len(opportunities)} opportunities using Selenium")
            
        except Exception as e:
            logger.error(f"Error during Selenium scraping: {e}")
            raise ScrapingError(f"Selenium scraping failed: {e}")
        
        return opportunities
    
    def _scrape_with_requests(self, max_pages: int, start_date: datetime, end_date: datetime) -> List[Opportunity]:
        """Fallback scraping method using requests (for non-JavaScript content)."""
        opportunities = []
        page = 1
        
        try:
            while page <= max_pages:
                logger.info(f"Scraping ESBD page {page} using requests")
                
                # Get opportunities from current page
                page_opportunities = self._scrape_opportunity_list(page, start_date, end_date)
                
                if not page_opportunities:
                    logger.info(f"No more opportunities found on page {page}")
                    break
                
                # Process each opportunity using enhanced extraction
                for opp_data in page_opportunities:
                    try:
                        opportunity = self._process_opportunity(opp_data)
                        if opportunity:
                            opportunities.append(opportunity)
                    except Exception as e:
                        logger.error(f"Error processing opportunity: {e}")
                        continue
                
                # Check if there's a next page
                if not self._has_next_page(page):
                    logger.info("No next page found, stopping pagination")
                    break
                
                page += 1
                
                # Rate limiting
                self.rate_limiter.wait()
            
            return opportunities
            
        except ScrapingError as e:
            logger.warning(f"ESBD portal not accessible via requests: {e}")
            logger.info("Returning empty list - portal may be temporarily unavailable")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in requests scraping: {e}")
            return []
    
    def _extract_opportunities_from_dynamic_content(self, soup: BeautifulSoup, start_date: datetime, end_date: datetime) -> List[ESBDOpportunity]:
        """Extract opportunities from dynamically loaded content."""
        opportunities = []
        
        # Approach 1: Look for ESBD-specific opportunity rows (primary method)
        opportunity_rows = soup.find_all('div', class_='esbd-result-row')
        logger.info(f"Found {len(opportunity_rows)} ESBD opportunity rows")
        
        for row in opportunity_rows:
            opp_data = self._extract_opportunity_from_esbd_row(row, start_date, end_date)
            if opp_data:
                opportunities.append(opp_data)
        
        # Approach 2: Look for any table structures (fallback)
        if not opportunities:
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables in dynamic content")
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) > 1:  # Skip tables with only header
                    logger.info(f"Analyzing table with {len(rows)} rows")
                    for row in rows[1:]:  # Skip header row
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:  # Minimum cells for opportunity data
                            opp_data = self._extract_opportunity_from_table_row(cells)
                            if opp_data:
                                opportunities.append(opp_data)
        
        # Approach 3: Look for div structures that might contain opportunities (fallback)
        if not opportunities:
            opportunity_containers = soup.find_all('div', class_=re.compile(r'opportunity|bid|solicitation|procurement', re.I))
            logger.info(f"Found {len(opportunity_containers)} potential opportunity containers")
            
            for container in opportunity_containers:
                opp_data = self._extract_opportunity_from_container(container)
                if opp_data:
                    opportunities.append(opp_data)
        
        # Approach 4: Look for any links that might lead to opportunities (fallback)
        if not opportunities:
            links = soup.find_all('a', href=True)
            opportunity_links = []
            for link in links:
                href = link['href']
                text = link.get_text(strip=True)
                if any(word in href.lower() or word in text.lower() 
                       for word in ['bid', 'solicitation', 'opportunity', 'rfp', 'rfq']):
                    opportunity_links.append((href, text))
            
            logger.info(f"Found {len(opportunity_links)} potential opportunity links")
            
            # Create opportunities from links
            for href, text in opportunity_links[:10]:  # Limit to first 10
                opp_data = self._create_opportunity_from_link(href, text)
                if opp_data:
                    opportunities.append(opp_data)
        
        logger.info(f"Total opportunities extracted from dynamic content: {len(opportunities)}")
        return opportunities
    
    def _extract_opportunity_from_table_row(self, cells) -> Optional[ESBDOpportunity]:
        """Extract opportunity data from a table row."""
        try:
            if len(cells) < 3:
                return None
            
            # Try to extract basic information from cells
            title = cells[0].get_text(strip=True) if len(cells) > 0 else ""
            agency = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            deadline_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            
            if not title or title.lower() in ['title', 'description', '']:
                return None
            
            # Parse deadline
            submission_deadline = None
            try:
                if deadline_text:
                    submission_deadline = datetime.strptime(deadline_text, '%m/%d/%Y')
            except:
                pass
            
            return ESBDOpportunity(
                title=title,
                description=title,  # Use title as description for now
                agency=agency,
                bid_number=f"ESBD-{hash(title) % 10000:04d}",  # Generate a bid number
                submission_deadline=submission_deadline,
                pre_bid_meeting=None,
                contact_name=None,
                contact_email=None,
                contact_phone=None,
                nigp_codes=[],
                wbe_requirements=[],
                document_urls=[],
                portal_url=self.base_url,
                raw_data={'source': 'table_row', 'cells': [cell.get_text(strip=True) for cell in cells]}
            )
        except Exception as e:
            logger.error(f"Error extracting opportunity from table row: {e}")
            return None
    
    def _extract_opportunity_from_container(self, container) -> Optional[ESBDOpportunity]:
        """Extract opportunity data from a container div."""
        try:
            # Look for title/heading
            title_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            title = title_elem.get_text(strip=True) if title_elem else ""
            
            if not title:
                return None
            
            # Look for other information
            text_content = container.get_text(strip=True)
            
            return ESBDOpportunity(
                title=title,
                description=text_content[:200] + "..." if len(text_content) > 200 else text_content,
                agency="Texas State Agency",  # Default
                bid_number=f"ESBD-{hash(title) % 10000:04d}",
                submission_deadline=None,
                pre_bid_meeting=None,
                contact_name=None,
                contact_email=None,
                contact_phone=None,
                nigp_codes=[],
                wbe_requirements=[],
                document_urls=[],
                portal_url=self.base_url,
                raw_data={'source': 'container', 'content': text_content}
            )
        except Exception as e:
            logger.error(f"Error extracting opportunity from container: {e}")
            return None

    def _extract_opportunity_from_esbd_row(self, row, start_date: datetime, end_date: datetime) -> Optional[ESBDOpportunity]:
        """Extract opportunity data from an ESBD-specific result row."""
        try:
            # Extract title and link from esbd-result-title
            title_elem = row.find('div', class_='esbd-result-title')
            if not title_elem:
                return None
            
            title_link = title_elem.find('a')
            if not title_link:
                return None
            
            title = title_link.get_text(strip=True)
            detail_url = title_link.get('href', '')
            
            # Make URL absolute if it's relative
            if detail_url.startswith('/'):
                detail_url = f"http://www.txsmartbuy.gov{detail_url}"
            
            # Extract data from esbd-result-body-columns
            body_columns = row.find('div', class_='esbd-result-body-columns')
            if not body_columns:
                return None
            
            # Extract data from first column
            first_column = body_columns.find('div', class_='esbd-result-column')
            solicitation_id = ""
            due_date = None
            due_time = ""
            
            if first_column:
                # Extract Solicitation ID - look for p tag containing "Solicitation ID:"
                for p_tag in first_column.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Solicitation ID:' in text:
                        solicitation_id = text.replace('Solicitation ID:', '').strip()
                        break
                
                # Extract Due Date
                for p_tag in first_column.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Due Date:' in text:
                        due_date_text = text.replace('Due Date:', '').strip()
                        due_date = self._parse_date(due_date_text)
                        break
                
                # Extract Due Time
                for p_tag in first_column.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Due Time:' in text:
                        due_time = text.replace('Due Time:', '').strip()
                        break
            
            # Extract data from second column
            second_column = body_columns.find_all('div', class_='esbd-result-column')[1] if len(body_columns.find_all('div', class_='esbd-result-column')) > 1 else None
            agency_number = ""
            status = ""
            posting_date = None
            
            if second_column:
                # Extract Agency/Texas SmartBuy Member Number
                for p_tag in second_column.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Agency/Texas SmartBuy Member Number:' in text:
                        agency_number = text.replace('Agency/Texas SmartBuy Member Number:', '').strip()
                        break
                
                # Extract Status
                for p_tag in second_column.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Status:' in text:
                        status = text.replace('Status:', '').strip()
                        break
                
                # Extract Posting Date
                for p_tag in second_column.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Posting Date:' in text:
                        posting_date_text = text.replace('Posting Date:', '').strip()
                        posting_date = self._parse_date(posting_date_text)
                        break
            
            # Extract data from secondary section
            secondary_section = body_columns.find('div', class_='esbd-result-body-secondary')
            created_date = None
            last_updated = None
            
            if secondary_section:
                # Extract Created Date
                for p_tag in secondary_section.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Created Date:' in text:
                        created_date_text = text.replace('Created Date:', '').strip()
                        created_date = self._parse_date(created_date_text)
                        break
                
                # Extract Last Updated
                for p_tag in secondary_section.find_all('p'):
                    text = p_tag.get_text(strip=True)
                    if 'Last Updated:' in text:
                        last_updated_text = text.replace('Last Updated:', '').strip()
                        last_updated = self._parse_date(last_updated_text)
                        break
            
            # Create description from available data
            description_parts = []
            if solicitation_id:
                description_parts.append(f"Solicitation ID: {solicitation_id}")
            if agency_number:
                description_parts.append(f"Agency: {agency_number}")
            if status:
                description_parts.append(f"Status: {status}")
            if due_date:
                description_parts.append(f"Due Date: {due_date.strftime('%m/%d/%Y')}")
            if due_time:
                description_parts.append(f"Due Time: {due_time}")
            
            description = " | ".join(description_parts)
            
            # Filter by date range
            if start_date and end_date:
                if due_date and due_date < start_date:
                    logger.debug(f"Skipping opportunity due to date: {due_date} < {start_date}")
                    return None
                if due_date and due_date > end_date:
                    logger.debug(f"Skipping opportunity due to date: {due_date} > {end_date}")
                    return None
            
            return ESBDOpportunity(
                title=title,
                description=description,
                agency=agency_number,
                bid_number=solicitation_id,
                submission_deadline=due_date,
                pre_bid_meeting=None,
                contact_name=None,
                contact_email=None,
                contact_phone=None,
                nigp_codes=[],
                wbe_requirements=[],
                document_urls=[detail_url] if detail_url else [],
                portal_url=detail_url,
                raw_data={
                    'status': status,
                    'posting_date': posting_date,
                    'created_date': created_date,
                    'last_updated': last_updated,
                    'due_time': due_time,
                    'row_html': str(row)
                }
            )
            
        except Exception as e:
            logger.error(f"Error extracting opportunity from ESBD row: {e}")
            return None
    
    def _create_opportunity_from_link(self, href: str, text: str) -> Optional[ESBDOpportunity]:
        """Create opportunity data from a link."""
        try:
            if not text or len(text) < 5:
                return None
            
            return ESBDOpportunity(
                title=text,
                description=text,
                agency="Texas State Agency",
                bid_number=f"ESBD-{hash(text) % 10000:04d}",
                submission_deadline=None,
                pre_bid_meeting=None,
                contact_name=None,
                contact_email=None,
                contact_phone=None,
                nigp_codes=[],
                wbe_requirements=[],
                document_urls=[href] if href.startswith('http') else [],
                portal_url=self.base_url,
                raw_data={'source': 'link', 'href': href, 'text': text}
            )
        except Exception as e:
            logger.error(f"Error creating opportunity from link: {e}")
            return None

    def _scrape_opportunity_list(self, page: int, start_date: datetime, end_date: datetime) -> List[ESBDOpportunity]:
        """
        Scrape opportunity list from a specific page.
        
        Args:
            page: Page number to scrape
            
        Returns:
            List of ESBDOpportunity objects
        """
        try:
            # Build URL with pagination parameters
            if page == 1:
                url = self.search_url
            else:
                # Add page parameter to URL
                separator = '&' if '?' in self.search_url else '?'
                url = f"{self.search_url}{separator}page={page}"
            
            response = self.http_client.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            opportunities = []
            
            # Find opportunity items - try multiple possible selectors
            opportunity_selectors = [
                self.selectors.get('opportunity_list', '.opportunity-item'),
                'table tr',  # Common table structure
                '.bid-item',
                '.solicitation-item',
                '.procurement-item'
            ]
            
            opportunity_elements = []
            for selector in opportunity_selectors:
                elements = soup.select(selector)
                if elements:
                    opportunity_elements = elements
                    logger.info(f"Found {len(elements)} opportunities using selector: {selector}")
                    break
            
            if not opportunity_elements:
                logger.warning("No opportunity elements found with any selector")
                return []
            
            # Extract opportunity data from each element
            for element in opportunity_elements:
                try:
                    opp_data = self._extract_opportunity_data(element, start_date, end_date)
                    if opp_data:
                        opportunities.append(opp_data)
                except Exception as e:
                    logger.error(f"Error extracting opportunity data: {e}")
                    continue
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Error scraping opportunity list: {e}")
            raise ScrapingError(f"Failed to scrape opportunity list: {e}")
    
    def _extract_opportunity_data(self, element, start_date: datetime, end_date: datetime) -> Optional[ESBDOpportunity]:
        """
        Extract opportunity data from a single element using enhanced extraction.
        
        Args:
            element: BeautifulSoup element containing opportunity data
            
        Returns:
            ESBDOpportunity object or None if extraction fails
        """
        try:
            # Use enhanced extraction from base class
            standardized_data = self.extract_opportunity_data(element)
            
            # Convert to ESBD-specific format with portal-specific fallbacks
            opp_data = self._convert_to_esbd_opportunity(standardized_data, element)
            
            if opp_data:
                # Calculate advantage score using enhanced scoring
                opp_data.raw_data['advantage_score'] = self._calculate_advantage_score(opp_data)
                
                # Filter by date range
                if start_date and end_date:
                    if opp_data.submission_deadline and opp_data.submission_deadline < start_date:
                        logger.debug(f"Skipping opportunity due to date: {opp_data.submission_deadline} < {start_date}")
                        return None
                    if opp_data.submission_deadline and opp_data.submission_deadline > end_date:
                        logger.debug(f"Skipping opportunity due to date: {opp_data.submission_deadline} > {end_date}")
                        return None
            
            return opp_data
            
        except Exception as e:
            logger.error(f"Error extracting opportunity data: {e}")
            return None
    
    def _extract_portal_specific_data(self, element) -> Optional[ESBDOpportunity]:
        """
        Extract ESBD-specific data when standardized extraction fails.
        
        Args:
            element: BeautifulSoup element containing opportunity data
            
        Returns:
            ESBDOpportunity object or None if extraction fails
        """
        try:
            # Extract basic information using portal-specific selectors
            title = self._extract_text(element, self.selectors.get('title', '.opportunity-title'))
            if not title:
                return None
            
            description = self._extract_text(element, self.selectors.get('description', '.opportunity-description'))
            agency = self._extract_text(element, self.selectors.get('agency', '.agency-name'))
            bid_number = self._extract_text(element, self.selectors.get('bid_number', '.bid-number'))
            submission_deadline = self._extract_date(element, self.selectors.get('deadline', '.submission-deadline'))
            
            # Extract contact information
            contact_info = self._extract_contact_info(element)
            
            # Extract NIGP codes
            nigp_codes = self._extract_nigp_codes(element)
            
            # Extract WBE requirements
            wbe_requirements = self._extract_wbe_requirements(element)
            
            # Extract document URLs
            document_urls = self._extract_document_urls(element)
            
            # Extract detail URL
            detail_url = self._extract_detail_url(element)
            
            # Create ESBDOpportunity object
            opp_data = ESBDOpportunity(
                title=title,
                description=description or '',
                agency=agency or '',
                bid_number=bid_number or '',
                submission_deadline=submission_deadline,
                pre_bid_meeting=None,  # ESBD doesn't typically have pre-bid meetings
                contact_name=contact_info.get('name'),
                contact_email=contact_info.get('email'),
                contact_phone=contact_info.get('phone'),
                nigp_codes=nigp_codes,
                wbe_requirements=wbe_requirements,
                document_urls=document_urls,
                portal_url=detail_url,
                raw_data={
                    'source': 'ESBD',
                    'extraction_method': 'portal_specific'
                }
            )
            
            return opp_data
            
        except Exception as e:
            logger.error(f"Error extracting portal-specific data: {e}")
            return None
    
    def _convert_to_esbd_opportunity(self, standardized_data: Dict[str, Any], element) -> Optional[ESBDOpportunity]:
        """
        Convert standardized data to ESBD-specific format with fallbacks.
        
        Args:
            standardized_data: Standardized opportunity data
            element: Original BeautifulSoup element for fallback extraction
            
        Returns:
            ESBDOpportunity object or None if conversion fails
        """
        try:
            logger.debug(f"Standardized data: {standardized_data}")
            
            # If standardized extraction returned data, use it as base
            if standardized_data and isinstance(standardized_data, dict):
                # Start with standardized data
                opp_data = ESBDOpportunity(
                    title=standardized_data.get('title', ''),
                    description=standardized_data.get('description', ''),
                    agency=standardized_data.get('agency', ''),
                    bid_number=standardized_data.get('bid_number', ''),
                    submission_deadline=standardized_data.get('submission_deadline'),
                    pre_bid_meeting=standardized_data.get('pre_bid_meeting'),
                    contact_name=standardized_data.get('contact_name'),
                    contact_email=standardized_data.get('contact_email'),
                    contact_phone=standardized_data.get('contact_phone'),
                    nigp_codes=standardized_data.get('nigp_codes', []),
                    wbe_requirements=standardized_data.get('wbe_requirements', []),
                    document_urls=standardized_data.get('document_urls', []),
                    portal_url=standardized_data.get('portal_url', ''),
                    raw_data={
                        'source': 'ESBD',
                        'extraction_method': 'standardized',
                        'quality_score': standardized_data.get('quality_score', 0.0)
                    }
                )
                
                logger.debug(f"Initial title: {opp_data.title}")
                logger.debug(f"Initial NIGP codes: {opp_data.nigp_codes}")
                
                # Check if title is missing or invalid
                title_valid = opp_data.title and isinstance(opp_data.title, str) and len(opp_data.title.strip()) > 0
                
                # If title is missing, try to extract it using portal-specific method
                if not title_valid:
                    logger.debug("Title missing or invalid, trying portal-specific extraction")
                    title = self._extract_text(element, self.selectors.get('title', '.opportunity-title'))
                    if title:
                        opp_data.title = title
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added title: {title}")
                
                # Check if description is missing or invalid
                description_valid = opp_data.description and isinstance(opp_data.description, str) and len(opp_data.description.strip()) > 0
                
                # If description is missing, try to extract it using portal-specific method
                if not description_valid:
                    logger.debug("Description missing or invalid, trying portal-specific extraction")
                    description = self._extract_text(element, self.selectors.get('description', '.opportunity-description'))
                    if description:
                        opp_data.description = description
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added description: {description}")
                
                # Check if agency is missing or invalid
                agency_valid = opp_data.agency and isinstance(opp_data.agency, str) and len(opp_data.agency.strip()) > 0
                
                # If agency is missing, try to extract it using portal-specific method
                if not agency_valid:
                    logger.debug("Agency missing or invalid, trying portal-specific extraction")
                    agency = self._extract_text(element, self.selectors.get('agency', '.agency-name'))
                    if agency:
                        opp_data.agency = agency
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added agency: {agency}")
                
                # Check if bid number is missing or invalid
                bid_number_valid = opp_data.bid_number and isinstance(opp_data.bid_number, str) and len(opp_data.bid_number.strip()) > 0
                
                # If bid number is missing, try to extract it using portal-specific method
                if not bid_number_valid:
                    logger.debug("Bid number missing or invalid, trying portal-specific extraction")
                    bid_number = self._extract_text(element, self.selectors.get('bid_number', '.bid-number'))
                    if bid_number:
                        opp_data.bid_number = bid_number
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added bid number: {bid_number}")
                
                # Check if submission deadline is missing or invalid
                deadline_valid = opp_data.submission_deadline is not None
                
                # If submission deadline is missing, try to extract it using portal-specific method
                if not deadline_valid:
                    logger.debug("Submission deadline missing or invalid, trying portal-specific extraction")
                    deadline = self._extract_date(element, self.selectors.get('deadline', '.submission-deadline'))
                    if deadline:
                        opp_data.submission_deadline = deadline
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added submission deadline: {deadline}")
                
                # Check if contact information is missing or invalid
                contact_info_valid = (
                    opp_data.contact_name or 
                    opp_data.contact_email or 
                    opp_data.contact_phone
                )
                
                # If contact information is missing, try to extract it using portal-specific method
                if not contact_info_valid:
                    logger.debug("Contact information missing or invalid, trying portal-specific extraction")
                    contact_info = self._extract_contact_info(element)
                    if contact_info.get('name'):
                        opp_data.contact_name = contact_info['name']
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added contact name: {contact_info['name']}")
                    if contact_info.get('email'):
                        opp_data.contact_email = contact_info['email']
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added contact email: {contact_info['email']}")
                    if contact_info.get('phone'):
                        opp_data.contact_phone = contact_info['phone']
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added contact phone: {contact_info['phone']}")
                
                # Check if NIGP codes are valid (should be a list of strings, not a single string)
                nigp_codes_valid = (
                    isinstance(opp_data.nigp_codes, list) and 
                    len(opp_data.nigp_codes) > 0 and 
                    all(isinstance(code, str) and len(code) > 0 for code in opp_data.nigp_codes)
                )
                
                # If NIGP codes are missing or invalid, try to extract them using portal-specific method
                if not nigp_codes_valid:
                    logger.debug("NIGP codes missing or invalid, trying portal-specific extraction")
                    nigp_codes = self._extract_nigp_codes(element)
                    if nigp_codes:
                        opp_data.nigp_codes = nigp_codes
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added NIGP codes: {nigp_codes}")
                
                # Check if WBE requirements are valid (should be a list of strings, not a single string)
                wbe_requirements_valid = (
                    isinstance(opp_data.wbe_requirements, list) and 
                    len(opp_data.wbe_requirements) > 0 and 
                    all(isinstance(req, str) and len(req) > 0 for req in opp_data.wbe_requirements)
                )
                
                # If WBE requirements are missing or invalid, try to extract them using portal-specific method
                if not wbe_requirements_valid:
                    logger.debug("WBE requirements missing or invalid, trying portal-specific extraction")
                    wbe_requirements = self._extract_wbe_requirements(element)
                    if wbe_requirements:
                        opp_data.wbe_requirements = wbe_requirements
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added WBE requirements: {wbe_requirements}")
                
                # Check if document URLs are missing or invalid
                document_urls_valid = (
                    isinstance(opp_data.document_urls, list) and 
                    len(opp_data.document_urls) > 0
                )
                
                # If document URLs are missing, try to extract them using portal-specific method
                if not document_urls_valid:
                    logger.debug("Document URLs missing or invalid, trying portal-specific extraction")
                    document_urls = self._extract_document_urls(element)
                    if document_urls:
                        opp_data.document_urls = document_urls
                        opp_data.raw_data['extraction_method'] = 'hybrid'
                        logger.debug(f"Added document URLs: {document_urls}")
                
                # Final validation: ensure we have a valid title
                if not opp_data.title or len(opp_data.title.strip()) == 0:
                    logger.debug("No valid title found after all extraction attempts")
                    return None
                
                return opp_data
            
            # Fallback to portal-specific extraction
            logger.debug("No standardized data, using portal-specific extraction")
            return self._extract_portal_specific_data(element)
            
        except Exception as e:
            logger.error(f"Error converting to ESBD opportunity: {e}")
            return None

    def _extract_text(self, element, selector: str) -> Optional[str]:
        """Extract text from element using CSS selector."""
        try:
            found_element = element.select_one(selector)
            return found_element.get_text(strip=True) if found_element else None
        except Exception as e:
            logger.debug(f"Error extracting text with selector '{selector}': {e}")
            return None

    def _extract_date(self, element, selector: str) -> Optional[datetime]:
        """Extract and parse date from element using CSS selector."""
        try:
            date_text = self._extract_text(element, selector)
            if not date_text:
                return None
            
            # ESBD date format: MM/DD/YYYY or MM/DD/YYYY HH:MM AM/PM
            date_patterns = [
                r'(\d{1,2}/\d{1,2}/\d{4})',
                r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+(AM|PM))',
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{1,2}-\d{1,2}-\d{4})'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_text)
                if match:
                    date_str = match.group(1)
                    try:
                        # Try different date formats
                        for fmt in ['%m/%d/%Y', '%m/%d/%Y %I:%M %p', '%Y-%m-%d', '%m-%d-%Y']:
                            try:
                                return datetime.strptime(date_str, fmt)
                            except ValueError:
                                continue
                    except Exception as e:
                        logger.debug(f"Error parsing date '{date_str}': {e}")
                        continue
            
            logger.warning(f"Could not parse date from text: {date_text}")
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting date with selector '{selector}': {e}")
            return None

    def _extract_contact_info(self, element) -> Dict[str, Optional[str]]:
        """Extract contact information from element."""
        contact_info = {}
        
        try:
            contact_element = element.select_one(self.selectors.get('contact_info', '.contact-information'))
            if not contact_element:
                logger.debug("No contact element found")
                return contact_info
            
            text = contact_element.get_text()
            logger.debug(f"Contact element text: {text}")
            
            # Extract email
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            email_match = re.search(email_pattern, text)
            contact_info['email'] = email_match.group() if email_match else None
            
            # Extract phone
            phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
            phone_match = re.search(phone_pattern, text)
            contact_info['phone'] = phone_match.group() if phone_match else None
            
            # Extract name (not implemented yet)
            contact_info['name'] = None
            
            logger.debug(f"Extracted contact info: {contact_info}")
            return contact_info
            
        except Exception as e:
            logger.debug(f"Error extracting contact info: {e}")
            return {}

    def _extract_nigp_codes(self, element) -> List[str]:
        """Extract NIGP codes from element."""
        nigp_codes = []
        
        try:
            nigp_element = element.select_one(self.selectors.get('nigp_codes', '.nigp-codes'))
            if not nigp_element:
                logger.debug("No NIGP element found")
                return nigp_codes
            
            # Look for NIGP codes in text (format: XXX-XX or XXXX-XX)
            text = nigp_element.get_text()
            logger.debug(f"NIGP element text: {text}")
            
            # Pattern for NIGP codes: 2-4 digits, hyphen, 2 digits
            nigp_pattern = r'\b(\d{2,4}-\d{2})\b'
            nigp_matches = re.findall(nigp_pattern, text)
            logger.debug(f"NIGP matches with hyphen: {nigp_matches}")
            
            for match in nigp_matches:
                if match not in nigp_codes:
                    nigp_codes.append(match)
            
            # Also look for NIGP codes without hyphens (6 digits total)
            nigp_no_hyphen_pattern = r'\b(\d{6})\b'
            nigp_no_hyphen_matches = re.findall(nigp_no_hyphen_pattern, text)
            logger.debug(f"NIGP matches without hyphen: {nigp_no_hyphen_matches}")
            
            for match in nigp_no_hyphen_matches:
                # Format as XXX-XX
                formatted_code = f"{match[:3]}-{match[3:]}"
                if formatted_code not in nigp_codes:
                    nigp_codes.append(formatted_code)
            
            logger.debug(f"Final NIGP codes: {nigp_codes}")
            return nigp_codes
            
        except Exception as e:
            logger.debug(f"Error extracting NIGP codes: {e}")
            return nigp_codes

    def _extract_wbe_requirements(self, element) -> List[str]:
        """Extract WBE requirements from element."""
        wbe_requirements = []
        try:
            wbe_element = element.select_one(self.selectors.get('wbe_requirements', '.wbe-requirements'))
            if not wbe_element:
                return wbe_requirements
            # Look for WBE-related keywords
            text = wbe_element.get_text().lower()
            wbe_keywords = [
                'wbe', 'woman', 'women', 'minority', 'disadvantaged', 'hub',
                'dbe', 'mbe', 'sbe', 'veteran', 'disabled', 'small business', 'set-aside', 'mwbe'
            ]
            for keyword in wbe_keywords:
                if keyword in text:
                    wbe_requirements.append(keyword)
            return wbe_requirements
        except Exception as e:
            logger.debug(f"Error extracting WBE requirements: {e}")
            return wbe_requirements

    def _extract_document_urls(self, element) -> List[str]:
        """Extract document URLs from element."""
        document_urls = []
        
        try:
            selector = self.selectors.get('document_links', 'a[href*=".pdf"], a[href*=".doc"], a[href*=".docx"]')
            logger.debug(f"Extracting document URLs using selector: {selector}")
            document_links = element.select(selector)
            logger.debug(f"Found {len(document_links)} document links")
            
            for link in document_links:
                href = link.get('href')
                logger.debug(f"Document link href: {href}")
                if href:
                    # Make relative URLs absolute
                    if href.startswith('/'):
                        base_url = getattr(self.portal_config, 'base_url', self.base_url)
                        full_url = urljoin(base_url, href)
                        document_urls.append(full_url)
                        logger.debug(f"Added document URL: {full_url}")
                    elif href.startswith('http'):
                        document_urls.append(href)
                        logger.debug(f"Added document URL: {href}")
                    else:
                        full_url = urljoin(self.document_base_url, href)
                        document_urls.append(full_url)
                        logger.debug(f"Added document URL: {full_url}")
            
            logger.debug(f"Final document URLs: {document_urls}")
            return document_urls
            
        except Exception as e:
            logger.debug(f"Error extracting document URLs: {e}")
            return document_urls

    def _extract_detail_url(self, element) -> str:
        """Extract detail page URL from element."""
        try:
            # Look for detail link
            detail_link = element.select_one('a[href*="detail"], a[href*="view"], .detail-link a')
            if detail_link:
                href = detail_link.get('href')
                if href:
                    # Always join with the full base_url path for detail links
                    base_url = 'https://comptroller.texas.gov/purchasing/bids/'
                    return urljoin(base_url, href.lstrip('/'))
            # Fallback: construct detail URL from bid number
            bid_number = self._extract_text(element, self.selectors.get('bid_number', '.bid-number'))
            if bid_number:
                return f"{self.detail_url}?id={bid_number}"
            return self.base_url
        except Exception as e:
            logger.debug(f"Error extracting detail URL: {e}")
            return self.base_url

    def _has_next_page(self, current_page: int) -> bool:
        """Check if there's a next page available."""
        try:
            # ESBD typically shows pagination info
            # This is a simplified check - in practice, you'd parse the pagination
            return current_page < 10  # Assume max 10 pages for now
            
        except Exception as e:
            logger.debug(f"Error checking for next page: {e}")
            return False

    def _process_opportunity(self, opp_data: ESBDOpportunity) -> Optional[Opportunity]:
        """
        Process ESBD opportunity data into database model.
        
        Args:
            opp_data: ESBD opportunity data
            
        Returns:
            Opportunity object or None if processing fails
        """
        try:
            # Calculate advantage score using enhanced scoring
            advantage_score = self._calculate_advantage_score(opp_data)
            
            # Create contact info JSON
            contact_info = {
                'name': opp_data.contact_name,
                'email': opp_data.contact_email,
                'phone': opp_data.contact_phone,
                'calculated_advantage_score': advantage_score,
                'wbe_requirements': opp_data.wbe_requirements
            }
            
            # Create Opportunity object
            opportunity = Opportunity(
                external_id=opp_data.bid_number,
                source_portal="ESBD",
                title=opp_data.title,
                description_short=opp_data.description[:200] if opp_data.description else "",
                description_full=opp_data.description,
                issuing_entity_name=opp_data.agency,
                status="Open",  # ESBD opportunities are typically open
                post_date=None,  # ESBD doesn't always provide post date
                due_date=opp_data.submission_deadline,
                opportunity_url=opp_data.portal_url,
                estimated_value=None,  # ESBD doesn't always provide estimated value
                contact_info=contact_info
            )
            
            # Download documents
            for doc_url in opp_data.document_urls:
                try:
                    document = self._download_document(doc_url, opportunity)
                    if document:
                        # Add document to opportunity's documents list
                        opportunity.documents.append(document)
                        logger.info(f"Downloaded document: {document.document_name}")
                except Exception as e:
                    logger.error(f"Error downloading document {doc_url}: {e}")
                    continue
            
            # Add NIGP codes
            for nigp_code in opp_data.nigp_codes:
                try:
                    # Parse NIGP code format: "XXX-XX" or "XXXX-XX"
                    if '-' in nigp_code:
                        class_code, item_code = nigp_code.split('-', 1)
                    else:
                        # If no hyphen, treat as class code only
                        class_code = nigp_code
                        item_code = None
                    
                    nigp = NIGPCode(
                        opportunity_id=opportunity.id,
                        nigp_class_code=class_code,
                        nigp_item_code=item_code,
                        relevance_tier=1  # Perfect match
                    )
                    # Add to opportunity's NIGP codes list
                    opportunity.nigp_codes.append(nigp)
                except Exception as e:
                    logger.error(f"Error creating NIGP code {nigp_code}: {e}")
                    continue
            
            return opportunity
            
        except Exception as e:
            logger.error(f"Error processing opportunity: {e}")
            return None

    def _download_document(self, doc_url: str, opportunity: Opportunity) -> Optional[Document]:
        """
        Download document from URL and create Document object.
        
        Args:
            doc_url: Document URL
            opportunity: Associated opportunity
            
        Returns:
            Document object or None if download fails
        """
        try:
            response = self.http_client.get(doc_url, stream=True)
            response.raise_for_status()
            
            # Extract filename from URL or headers
            filename = self._extract_filename(doc_url, response.headers)
            
            # Determine document type from filename
            document_type = self._determine_document_type(filename)
            
            # Generate storage path
            storage_path = f"documents/esbd/{opportunity.external_id}/{filename}"
            
            # Create Document object
            document = Document(
                opportunity_id=opportunity.id,
                document_name=filename,
                document_url=doc_url,
                document_type=document_type,
                file_size=len(response.content),
                storage_path=storage_path,
                processing_status="Pending"
            )
            
            # In a real implementation, you'd save the file content to the storage path
            
            return document
            
        except requests.HTTPError as e:
            logger.error(f"HTTP error downloading document {doc_url}: {e}")
            raise DocumentDownloadError(f"Failed to download document {doc_url}: {e}")
        except Exception as e:
            logger.error(f"Error downloading document {doc_url}: {e}")
            return None

    def _extract_filename(self, url: str, headers: Dict[str, str]) -> str:
        """Extract filename from URL or headers."""
        try:
            # Try to get filename from Content-Disposition header (case-insensitive)
            content_disposition = headers.get('Content-Disposition', '') or headers.get('content-disposition', '')
            if content_disposition:
                # Look for filename in Content-Disposition header
                filename_match = re.search(r'filename="?([^"]+)"?', content_disposition)
                if filename_match:
                    return filename_match.group(1)
            # Try to get filename from URL
            parsed_url = urlparse(url)
            filename = parsed_url.path.split('/')[-1]
            if filename and '.' in filename:
                return filename
            # Fallback: generate filename
            return f"document_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        except Exception as e:
            logger.debug(f"Error extracting filename: {e}")
            return f"document_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

    def _determine_document_type(self, filename: str) -> str:
        """Determine document type from filename."""
        try:
            extension = filename.lower().split('.')[-1]
            type_mapping = {
                'pdf': 'PDF',
                'doc': 'DOC',
                'docx': 'DOCX',
                'xls': 'XLS',
                'xlsx': 'XLSX',
                'txt': 'TXT',
                'rtf': 'RTF'
            }
            return type_mapping.get(extension, 'UNKNOWN')
        except Exception as e:
            logger.debug(f"Error determining document type: {e}")
            return 'UNKNOWN'

    def _calculate_advantage_score(self, opp_data: ESBDOpportunity) -> float:
        """
        Calculate WBE advantage score for opportunity.
        
        Args:
            opp_data: ESBD opportunity data
            
        Returns:
            Advantage score between 0.0 and 1.0
        """
        try:
            score = 0.0
            
            # Base score for WBE requirements
            if opp_data.wbe_requirements:
                score += 0.3
            
            # Bonus for specific WBE keywords
            wbe_keywords = ['wbe', 'woman', 'women', 'minority', 'disadvantaged', 'hub', 'set-aside']
            for keyword in wbe_keywords:
                if any(keyword in req.lower() for req in opp_data.wbe_requirements):
                    score += 0.25  # Increased from 0.2
                    break
            
            # Bonus for percentage requirements
            for req in opp_data.wbe_requirements:
                if '%' in req:
                    score += 0.2
                    break
            
            # Bonus for multiple WBE requirements (indicates stronger preference)
            if len(opp_data.wbe_requirements) > 1:
                score += 0.15
            
            # Bonus for NIGP codes matching business categories
            # This would be enhanced with actual business category matching
            if opp_data.nigp_codes:
                score += 0.1
            
            # Cap score at 1.0
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"Error calculating advantage score: {e}")
            return 0.0

    def log_scraping_activity(self, opportunities_found: int, errors: int = 0):
        """Log scraping activity to database."""
        try:
            with get_db() as db:
                # Safely get rate limit delay
                rate_limit_delay = 0
                if hasattr(self, 'rate_limiter') and self.rate_limiter:
                    if hasattr(self.rate_limiter, 'delay'):
                        rate_limit_delay = self.rate_limiter.delay
                    elif hasattr(self.rate_limiter, 'requests_per_minute'):
                        rate_limit_delay = 60.0 / self.rate_limiter.requests_per_minute
                
                if errors == 0:
                    log_entry = ScrapingLog.create_success_log(
                        portal_name="ESBD",
                        url=self.search_url,
                        status_code=200,  # Assuming success
                        response_time_ms=0,  # Could be calculated if needed
                        user_agent=self.http_client.headers.get('User-Agent', ''),
                        robots_respected=True,
                        rate_limit_delay=rate_limit_delay
                    )
                else:
                    log_entry = ScrapingLog.create_failure_log(
                        portal_name="ESBD",
                        url=self.search_url,
                        error_message=f"Scraping failed with {errors} errors",
                        user_agent=self.http_client.headers.get('User-Agent', ''),
                        robots_respected=True,
                        rate_limit_delay=rate_limit_delay
                    )
                
                db.add(log_entry)
                db.commit()
                logger.info(f"Logged scraping activity: {opportunities_found} opportunities, {errors} errors")
                
        except Exception as e:
            logger.error(f"Error logging scraping activity: {e}") 