#!/usr/bin/env python3
"""
BeaconBid API Client - API-first approach with robust fallbacks.

This module implements a comprehensive client for accessing BeaconBid data
using multiple strategies in order of preference:
1. GraphQL API (if accessible)
2. REST Export API (if accessible) 
3. Enhanced HTML scraping
4. Selenium fallback (if needed)

All methods are configurable and testable.
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

from src.scrapers.exceptions import ScrapingError

logger = logging.getLogger(__name__)

class SimpleRateLimiter:
    """Simple rate limiter for API requests."""
    
    def __init__(self, requests_per_minute: int = 60, burst_size: int = 10):
        self.requests_per_minute = requests_per_minute
        self.burst_size = burst_size
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_time = 0.0
    
    def wait(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

@dataclass
class BeaconBidOpportunity:
    """Data class for BeaconBid opportunity data."""
    title: str
    agency: Optional[str] = None
    bid_number: Optional[str] = None
    deadline: Optional[str] = None
    submission_deadline: Optional[str] = None
    description: Optional[str] = None
    contact_info: Optional[Dict[str, str]] = None
    nigp_codes: Optional[List[str]] = None
    wbe_requirements: Optional[List[str]] = None
    document_urls: Optional[List[str]] = None
    url: Optional[str] = None
    source: str = "unknown"
    pre_bid_meeting: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    portal_url: Optional[str] = None
    raw_data: Optional[dict] = None

class BeaconBidAPIClient:
    """
    BeaconBid API Client with multiple access strategies.
    
    Implements API-first approach with robust fallbacks for maximum reliability.
    """
    
    def __init__(self, config: Union[Dict[str, Any], 'Config']):
        """
        Initialize BeaconBid API client.
        
        Args:
            config: Configuration dictionary or Config object containing API settings
        """
        # Convert Config object to dict if needed
        if hasattr(config, 'model_dump'):
            # It's a Pydantic model
            config_dict = config.model_dump()
        else:
            # It's already a dict
            config_dict = config
            
        self.config = config_dict
        self.base_url = config_dict.get('base_url', 'https://www.beaconbid.com')
        self.search_url = config_dict.get('search_url', 'https://www.beaconbid.com/solicitations/city-of-houston/open')
        self.selectors = config_dict.get('selectors', {})
        self.api_endpoints = config_dict.get('api_endpoints', {})
        
        # Initialize rate limiter
        self.rate_limiter = SimpleRateLimiter(
            requests_per_minute=config_dict.get('rate_limit', 60),
            burst_size=config_dict.get('burst_size', 10)
        )
        
        # Session for maintaining cookies and headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Selenium driver (initialized on demand)
        self.driver = None
        
        logger.info(f"BeaconBid API Client initialized for {self.base_url}")
    
    def get_opportunities(self, max_opportunities: int = 50) -> List[BeaconBidOpportunity]:
        """
        Get opportunities using the best available method.
        
        Args:
            max_opportunities: Maximum number of opportunities to retrieve
            
        Returns:
            List of BeaconBidOpportunity objects
        """
        opportunities = []
        
        # Check if Selenium is disabled
        selenium_config = self.config.get('selenium', {})
        selenium_enabled = selenium_config.get('enabled', True)
        
        # Check if Playwright is enabled
        playwright_config = self.config.get('playwright', {})
        playwright_enabled = playwright_config.get('enabled', False)
        
        # Try methods in order of preference
        methods = []
        
        # Add Playwright first if enabled (for widget-based content)
        if playwright_enabled:
            methods.append(('Playwright Rendering', self._get_opportunities_playwright))
            logger.info(f"Playwright enabled, added to methods list")
        else:
            logger.info(f"Playwright disabled in config")
            
        # Add other methods
        methods.extend([
            ('GraphQL API', self._get_opportunities_graphql),
            ('REST Export API', self._get_opportunities_rest),
            ('Enhanced HTML', self._get_opportunities_html),
        ])
        
        # Only add Selenium if enabled (as last resort)
        if selenium_enabled:
            methods.append(('Selenium Fallback', self._get_opportunities_selenium))
            
        logger.info(f"Total methods to try: {len(methods)}: {[method[0] for method in methods]}")
        
        for method_name, method_func in methods:
            try:
                logger.info(f"Attempting to get opportunities via {method_name}")
                opportunities = method_func(max_opportunities)
                
                if opportunities:
                    logger.info(f"Successfully retrieved {len(opportunities)} opportunities via {method_name}")
                    # Mark the source for each opportunity
                    for opp in opportunities:
                        opp.source = method_name
                    return opportunities
                else:
                    logger.warning(f"{method_name} returned no opportunities")
                    # Continue to next method instead of stopping
                    continue
                    
            except Exception as e:
                logger.warning(f"{method_name} failed: {e}")
                continue
        
        logger.error("All methods failed to retrieve opportunities")
        return []  # Return empty list instead of raising exception
    
    def _get_opportunities_graphql(self, max_opportunities: int) -> List[BeaconBidOpportunity]:
        """
        Get opportunities via GraphQL API.
        
        Args:
            max_opportunities: Maximum number of opportunities to retrieve
            
        Returns:
            List of BeaconBidOpportunity objects
        """
        graphql_url = urljoin(self.base_url, '/api/gql')
        
        # GraphQL query for solicitations (based on common patterns)
        query = """
        query GetSolicitations($agency: String, $status: String, $limit: Int) {
          solicitations(agency: $agency, status: $status, limit: $limit) {
            id
            title
            agency
            bidNumber
            deadline
            description
            contactInfo {
              name
              email
              phone
            }
            nigpCodes
            wbeRequirements
            documentUrls
            url
          }
        }
        """
        
        variables = {
            "agency": "city-of-houston",
            "status": "open",
            "limit": max_opportunities
        }
        
        payload = {
            "query": query,
            "variables": variables
        }
        
        try:
            self.rate_limiter.wait()
            response = self.session.post(
                graphql_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'solicitations' in data['data']:
                    return self._parse_graphql_opportunities(data['data']['solicitations'])
                else:
                    logger.warning("GraphQL response missing expected data structure")
                    return []
            else:
                logger.warning(f"GraphQL request failed with status {response.status_code}")
                return []
                
        except Exception as e:
            logger.warning(f"GraphQL API request failed: {e}")
            return []
    
    def _get_opportunities_rest(self, max_opportunities: int) -> List[BeaconBidOpportunity]:
        """
        Get opportunities via REST Export API.
        
        Args:
            max_opportunities: Maximum number of opportunities to retrieve
            
        Returns:
            List of BeaconBidOpportunity objects
        """
        export_url = urljoin(self.base_url, '/api/rest/agency/solicitations/export')
        
        params = {
            'agency': 'city-of-houston',
            'status': 'open',
            'format': 'json',
            'limit': max_opportunities
        }
        
        try:
            self.rate_limiter.wait()
            response = self.session.get(
                export_url,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '')
                if 'json' in content_type:
                    data = response.json()
                    return self._parse_rest_opportunities(data)
                else:
                    logger.warning(f"REST API returned non-JSON content: {content_type}")
                    return []
            else:
                logger.warning(f"REST API request failed with status {response.status_code}")
                return []
                
        except Exception as e:
            logger.warning(f"REST API request failed: {e}")
            return []
    
    def _get_opportunities_html(self, max_opportunities: int) -> List[BeaconBidOpportunity]:
        """
        Get opportunities via enhanced HTML scraping.
        
        Args:
            max_opportunities: Maximum number of opportunities to retrieve
            
        Returns:
            List of BeaconBidOpportunity objects
        """
        try:
            self.rate_limiter.wait()
            response = self.session.get(self.search_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for opportunities in the page
            opportunities = []
            
            # Try multiple selector strategies
            selector_strategies = [
                self.selectors.get('opportunity_list', '.solicitation-item'),
                '.opportunity-item',
                '.bid-item',
                '[data-opportunity]',
                '.card',
                '.listing-item'
            ]
            
            opportunity_elements = []
            for selector in selector_strategies:
                elements = soup.select(selector)
                if elements:
                    opportunity_elements = elements
                    logger.info(f"Found {len(elements)} opportunities using selector: {selector}")
                    break
            
            if not opportunity_elements:
                logger.warning("No opportunity elements found with any selector")
                return []
            
            # Extract data from each element
            for element in opportunity_elements[:max_opportunities]:
                try:
                    opportunity = self._extract_opportunity_from_element(element)
                    if opportunity:
                        opportunities.append(opportunity)
                except Exception as e:
                    logger.warning(f"Failed to extract opportunity from element: {e}")
                    continue
            
            return opportunities
            
        except Exception as e:
            logger.warning(f"HTML scraping failed: {e}")
            return []
    
    def _get_opportunities_selenium(self, max_opportunities: int) -> List[BeaconBidOpportunity]:
        """
        Get opportunities via Selenium fallback.
        
        Args:
            max_opportunities: Maximum number of opportunities to retrieve
            
        Returns:
            List of BeaconBidOpportunity objects
        """
        try:
            if not self.driver:
                self._init_selenium_driver()
            
            self.driver.get(self.search_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for opportunities to load
            opportunity_selector = self.selectors.get('opportunity_list', '.solicitation-item')
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, opportunity_selector))
                )
            except TimeoutException:
                logger.warning("Timeout waiting for opportunities to load")
                return []
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract opportunities from the parsed HTML
            opportunities = []
            
            # Try multiple selector strategies
            selector_strategies = [
                self.selectors.get('opportunity_list', '.solicitation-item'),
                '.opportunity-item',
                '.bid-item',
                '[data-opportunity]',
                '.card',
                '.listing-item'
            ]
            
            opportunity_elements = []
            for selector in selector_strategies:
                elements = soup.select(selector)
                if elements:
                    opportunity_elements = elements
                    logger.info(f"Found {len(elements)} opportunities using selector: {selector}")
                    break
            
            if not opportunity_elements:
                logger.warning("No opportunity elements found with any selector")
                return []
            
            # Extract data from each element
            for element in opportunity_elements[:max_opportunities]:
                try:
                    opportunity = self._extract_opportunity_from_element(element)
                    if opportunity:
                        opportunities.append(opportunity)
                except Exception as e:
                    logger.warning(f"Failed to extract opportunity from element: {e}")
                    continue
            
            return opportunities
            
        except Exception as e:
            logger.warning(f"Selenium fallback failed: {e}")
            return []
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None
    
    def _get_opportunities_playwright(self, max_opportunities: int) -> List[BeaconBidOpportunity]:
        """
        Get opportunities via Playwright for client-side rendered content.
        
        Args:
            max_opportunities: Maximum number of opportunities to retrieve
            
        Returns:
            List of BeaconBidOpportunity objects
        """
        try:
            import asyncio
            from playwright.sync_api import sync_playwright
            
            playwright_config = self.config.get('playwright', {})
            headless = playwright_config.get('headless', True)
            timeout = playwright_config.get('timeout', 30) * 1000  # Convert to milliseconds
            wait_time = playwright_config.get('wait_time', 5000)  # Wait time for content to load
            
            opportunities = []
            
            with sync_playwright() as p:
                # Launch browser
                browser = p.chromium.launch(headless=headless)
                page = browser.new_page()
                
                # Set timeout
                page.set_default_timeout(timeout)
                
                # Navigate to the widget URL
                widget_url = self.search_url
                logger.info(f"Loading Playwright page: {widget_url}")
                page.goto(widget_url)
                
                # Wait for content to load
                page.wait_for_timeout(wait_time)
                
                # Try to wait for specific elements that indicate the content is loaded
                try:
                    # Wait for any of the opportunity selectors to appear
                    opportunity_selector = self.selectors.get('opportunity_list', '.solicitation-item')
                    page.wait_for_selector(opportunity_selector, timeout=10000)
                except:
                    # If specific selector doesn't appear, continue with what we have
                    logger.warning("Opportunity selector not found, proceeding with available content")
                
                # Get the page content
                content = page.content()
                browser.close()
                
                # Parse the content with BeautifulSoup
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                
                # Look for opportunities in the page using the same logic as HTML method
                # Try multiple selector strategies
                selector_strategies = [
                    self.selectors.get('opportunity_list', '.solicitation-item'),
                    '.opportunity-item',
                    '.bid-item',
                    '[data-opportunity]',
                    '.card',
                    '.listing-item',
                    '[data-testid*="solicitation"]',
                    '.table-row',
                    'tr'
                ]
                
                opportunity_elements = []
                for selector in selector_strategies:
                    elements = soup.select(selector)
                    if elements:
                        opportunity_elements = elements
                        logger.info(f"Found {len(elements)} opportunities using selector: {selector}")
                        break
                
                if not opportunity_elements:
                    logger.warning("No opportunity elements found with any selector in Playwright content")
                    return []
                
                # Extract data from each element
                for element in opportunity_elements[:max_opportunities]:
                    try:
                        opportunity = self._extract_opportunity_from_element(element)
                        if opportunity:
                            opportunities.append(opportunity)
                    except Exception as e:
                        logger.warning(f"Failed to extract opportunity from element: {e}")
                        continue
                
                return opportunities
                
        except ImportError:
            logger.error("Playwright not available - install with: pip install playwright")
            return []
        except Exception as e:
            logger.warning(f"Playwright scraping failed: {e}")
            return []
    
    def _init_selenium_driver(self):
        """Initialize Selenium WebDriver."""
        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            
            self.driver = webdriver.Chrome(options=options)
            logger.info("Selenium WebDriver initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Selenium WebDriver: {e}")
            raise
    
    def _parse_graphql_opportunities(self, data: List[Dict[str, Any]]) -> List[BeaconBidOpportunity]:
        """Parse opportunities from GraphQL response."""
        opportunities = []
        
        for item in data:
            try:
                opportunity = BeaconBidOpportunity(
                    title=item.get('title', ''),
                    agency=item.get('agency'),
                    bid_number=item.get('bidNumber'),
                    deadline=item.get('deadline'),
                    description=item.get('description'),
                    contact_info=item.get('contactInfo'),
                    nigp_codes=item.get('nigpCodes', []),
                    wbe_requirements=item.get('wbeRequirements', []),
                    document_urls=item.get('documentUrls', []),
                    url=item.get('url'),
                    source='GraphQL API'
                )
                opportunities.append(opportunity)
            except Exception as e:
                logger.warning(f"Failed to parse GraphQL opportunity: {e}")
                continue
        
        return opportunities
    
    def _parse_rest_opportunities(self, data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> List[BeaconBidOpportunity]:
        """Parse opportunities from REST API response."""
        opportunities = []
        
        # Handle different response formats
        if isinstance(data, dict):
            items = data.get('solicitations', data.get('opportunities', data.get('data', [])))
        else:
            items = data
        
        if not isinstance(items, list):
            logger.warning("REST API response is not a list")
            return []
        
        for item in items:
            try:
                opportunity = BeaconBidOpportunity(
                    title=item.get('title', item.get('name', '')),
                    agency=item.get('agency', item.get('organization')),
                    bid_number=item.get('bid_number', item.get('bidNumber', item.get('solicitation_number'))),
                    deadline=item.get('deadline', item.get('due_date', item.get('dueDate'))),
                    description=item.get('description', item.get('summary')),
                    contact_info=item.get('contact_info', item.get('contactInfo')),
                    nigp_codes=item.get('nigp_codes', item.get('nigpCodes', [])),
                    wbe_requirements=item.get('wbe_requirements', item.get('wbeRequirements', [])),
                    document_urls=item.get('document_urls', item.get('documentUrls', [])),
                    url=item.get('url', item.get('link')),
                    source='REST API'
                )
                opportunities.append(opportunity)
            except Exception as e:
                logger.warning(f"Failed to parse REST opportunity: {e}")
                continue
        
        return opportunities
    
    def _extract_opportunity_from_element(self, element: BeautifulSoup) -> Optional[BeaconBidOpportunity]:
        """Extract opportunity data from a BeautifulSoup element."""
        try:
            # Extract basic information
            title = self._extract_text(element, self.selectors.get('title', '.solicitation-title'))
            if not title:
                return None
            
            agency = self._extract_text(element, self.selectors.get('agency', '.agency-name'))
            bid_number = self._extract_text(element, self.selectors.get('bid_number', '.solicitation-number'))
            deadline = self._extract_text(element, self.selectors.get('deadline', '.due-date'))
            description = self._extract_text(element, self.selectors.get('description', '.solicitation-description'))
            
            # Extract contact information
            contact_info = self._extract_contact_info(element)
            
            # Extract NIGP codes
            nigp_codes = self._extract_nigp_codes(element)
            
            # Extract WBE requirements
            wbe_requirements = self._extract_wbe_requirements(element)
            
            # Extract document URLs
            document_urls = self._extract_document_urls(element)
            
            # Extract opportunity URL
            url = self._extract_url(element)
            
            return BeaconBidOpportunity(
                title=title,
                agency=agency,
                bid_number=bid_number,
                deadline=deadline,
                description=description,
                contact_info=contact_info,
                nigp_codes=nigp_codes,
                wbe_requirements=wbe_requirements,
                document_urls=document_urls,
                url=url,
                source='HTML Scraping'
            )
            
        except Exception as e:
            logger.warning(f"Failed to extract opportunity from element: {e}")
            return None
    
    def _extract_text(self, element: BeautifulSoup, selector: str) -> Optional[str]:
        """Extract text from element using selector."""
        try:
            found = element.select_one(selector)
            if found:
                return found.get_text(strip=True)
        except Exception:
            pass
        return None
    
    def _extract_contact_info(self, element: BeautifulSoup) -> Optional[Dict[str, str]]:
        """Extract contact information from element."""
        contact_info = {}
        
        try:
            contact_element = element.select_one(self.selectors.get('contact_info', '.contact-information'))
            if contact_element:
                text = contact_element.get_text()
                
                # Extract email
                email_match = re.search(r'Email:\s*([^\s\n]+)', text)
                if email_match:
                    contact_info['email'] = email_match.group(1)
                
                # Extract phone - fix regex to capture full phone number
                phone_match = re.search(r'Phone:\s*([\(\)\d\s\-]+)', text)
                if phone_match:
                    contact_info['phone'] = phone_match.group(1).strip()
                
                # Extract name (first line that's not email or phone)
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('Email:') and not line.startswith('Phone:'):
                        contact_info['name'] = line
                        break
            
            return contact_info if contact_info else None
            
        except Exception as e:
            logger.warning(f"Failed to extract contact info: {e}")
            return None
    
    def _extract_nigp_codes(self, element: BeautifulSoup) -> List[str]:
        """Extract NIGP codes from element."""
        try:
            nigp_element = element.select_one(self.selectors.get('nigp_codes', '.nigp-codes'))
            if nigp_element:
                text = nigp_element.get_text()
                # Extract NIGP codes (format: XXX-XX)
                codes = re.findall(r'\d{3}-\d{2}', text)
                return codes
        except Exception as e:
            logger.warning(f"Failed to extract NIGP codes: {e}")
        return []
    
    def _extract_wbe_requirements(self, element: BeautifulSoup) -> List[str]:
        """Extract WBE requirements from element."""
        try:
            wbe_element = element.select_one(self.selectors.get('wbe_requirements', '.wbe-requirements'))
            if wbe_element:
                text = wbe_element.get_text()
                # Look for WBE-related keywords
                wbe_keywords = ['WBE', 'HUB', 'MWBE', 'Minority', 'Women', 'Disadvantaged']
                requirements = []
                for keyword in wbe_keywords:
                    if keyword.lower() in text.lower():
                        requirements.append(keyword)
                return requirements
        except Exception as e:
            logger.warning(f"Failed to extract WBE requirements: {e}")
        return []
    
    def _extract_document_urls(self, element: BeautifulSoup) -> List[str]:
        """Extract document URLs from element."""
        try:
            urls = []
            # Use the selector from config or fallback to common document patterns
            selector = self.selectors.get('document_urls', 'a[href*=".pdf"], a[href*=".doc"], a[href*=".docx"]')
            logger.debug(f"Using selector: {selector}")
            link_elements = element.select(selector)
            logger.debug(f"Found {len(link_elements)} link elements")
            for link in link_elements:
                href = link.get('href')
                logger.debug(f"Processing link with href: {href}")
                if href:
                    # Check if it's actually a document (not just any link)
                    if any(ext in href.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx']):
                        logger.debug(f"Adding document URL: {href}")
                        if not href.startswith('http'):
                            href = urljoin(self.base_url, href)
                        urls.append(href)
                    else:
                        logger.debug(f"Skipping non-document URL: {href}")
            logger.debug(f"Final document URLs: {urls}")
            return urls
        except Exception as e:
            logger.warning(f"Failed to extract document URLs: {e}")
        return []
    
    def _extract_url(self, element: BeautifulSoup) -> Optional[str]:
        """Extract opportunity URL from element."""
        try:
            link = element.select_one(self.selectors.get('url', 'a[href]'))
            if link:
                href = link.get('href')
                if href:
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    return href
        except Exception as e:
            logger.warning(f"Failed to extract URL: {e}")
        return None
    
    def close(self):
        """Clean up resources."""
        if hasattr(self, 'driver') and self.driver is not None:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
            finally:
                self.driver = None
        if hasattr(self, 'session'):
            self.session.close()
        logger.info("BeaconBid API Client closed") 