"""
San Antonio Procurement Portal Scraper

This module provides a scraper for the City of San Antonio's procurement opportunities portal.
The portal uses ASP.NET WebForms with a table-based layout for opportunity listings.
"""

import re
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
import requests

from .enhanced_base import EnhancedBaseScraper
from ..database.models import Opportunity, Document
from ..utils.logging import get_logger

logger = get_logger(__name__)


class SanAntonioScraper(EnhancedBaseScraper):
    """
    Scraper for the City of San Antonio procurement opportunities portal.
    
    The portal uses ASP.NET WebForms with a table-based layout. Each opportunity
    is displayed as a row in a table with columns for description, type, department,
    release date, blackout start date, and deadline.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the San Antonio scraper."""
        super().__init__(config, portal_name="SanAntonio")
        self.base_url = config.get("base_url", "https://webapp1.sanantonio.gov/BidContractOpps/Default.aspx")
        self.selectors = config.get("selectors", {})
        
    def scrape_opportunities(self) -> List[Opportunity]:
        """
        Scrape opportunities from the San Antonio procurement portal.
        
        Returns:
            List[Opportunity]: List of scraped opportunities
        """
        logger.info("Starting San Antonio opportunity scraping")
        
        try:
            # Fetch the main opportunities page
            response = self._make_sync_request(self.base_url)
            if not response:
                logger.error("Failed to fetch San Antonio opportunities page")
                return []
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract opportunities from the table
            opportunities = self._extract_opportunities_from_table(soup)
            
            logger.info(f"Successfully scraped {len(opportunities)} opportunities from San Antonio")
            return opportunities
            
        except Exception as e:
            logger.error(f"Error scraping San Antonio opportunities: {e}")
            return []
    
    def _make_sync_request(self, url: str) -> Optional[requests.Response]:
        """Make a synchronous HTTP request."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Error making request to {url}: {e}")
            return None
    
    def _extract_opportunities_from_table(self, soup: BeautifulSoup) -> List[Opportunity]:
        """
        Extract opportunities from the main table on the page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List[Opportunity]: List of extracted opportunities
        """
        opportunities = []
        
        # Find the main opportunities table
        table = soup.select_one("table#ContentPlaceHolder1_gvBidContractOpps")
        if not table:
            logger.warning("Could not find opportunities table")
            return opportunities
            
        # Get all rows except header and pagination
        rows = table.find_all("tr")[1:]  # Skip header row
        
        for row in rows:
            # Skip pagination row (last row with colspan)
            if row.find("td", attrs={"colspan": "6"}):
                continue
                
            opportunity = self._extract_opportunity_from_row(row)
            if opportunity:
                opportunities.append(opportunity)
                
        return opportunities
    
    def _extract_opportunity_from_row(self, row: Tag) -> Optional[Opportunity]:
        """
        Extract a single opportunity from a table row.
        
        Args:
            row: BeautifulSoup Tag representing a table row
            
        Returns:
            Optional[Opportunity]: Extracted opportunity or None if invalid
        """
        try:
            cells = row.find_all("td")
            if len(cells) < 6:
                logger.warning(f"Row has insufficient cells: {len(cells)}")
                return None
                
            # Extract basic information
            title_link = cells[0].find("a")
            if not title_link:
                logger.warning("No title link found in row")
                return None
                
            title_text = title_link.get_text(strip=True)
            detail_url = title_link.get("href")
            
            # Extract bid number from title (format: "6100019011 Description")
            bid_number = self._extract_bid_number(title_text)
            
            # Extract other fields
            opportunity_type = cells[1].get_text(strip=True)
            department = cells[2].get_text(strip=True)
            release_date = cells[3].get_text(strip=True)
            blackout_start = cells[4].get_text(strip=True)
            deadline_text = cells[5].get_text(strip=True)
            
            # Parse dates
            release_date_parsed = self._parse_date(release_date)
            deadline_parsed = self._parse_deadline(deadline_text)
            
            # Build full detail URL
            if detail_url:
                detail_url = urljoin(self.base_url, detail_url)
            
            # Create standardized data dictionary
            opportunity_data = {
                "title": title_text,
                "description_short": title_text,
                "description_full": title_text,  # Will be enhanced from detail page
                "external_id": bid_number,
                "issuing_entity_name": department,
                "post_date": release_date_parsed,
                "due_date": deadline_parsed,
                "source_portal": "SanAntonio",
                "opportunity_url": detail_url or self.base_url,
                "contact_info": {
                    "blackout_start": blackout_start,
                    "deadline_text": deadline_text,
                    "opportunity_type": opportunity_type
                }
            }
            
            # Use standardized extraction with portal-specific fallbacks
            opportunity = self._create_standardized_opportunity(opportunity_data)
            
            # Enhance with detail page information if available
            if detail_url:
                self._enhance_with_detail_page(opportunity, detail_url)
                
            return opportunity
            
        except Exception as e:
            logger.error(f"Error extracting opportunity from row: {e}")
            return None
    
    def _extract_bid_number(self, title_text: str) -> str:
        """
        Extract bid number from title text.
        
        Args:
            title_text: Full title text
            
        Returns:
            str: Extracted bid number
        """
        # Pattern: "6100019011 Description" or "23-04073 Description"
        match = re.match(r'^(\d{2}-\d{5}|\d{10})\s+(.+)$', title_text)
        if match:
            return match.group(1)
        
        # If no pattern match, generate a unique ID based on title hash
        import hashlib
        title_hash = hashlib.md5(title_text.encode()).hexdigest()[:8]
        return f"SA-{title_hash}"
    
    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """
        Parse date text to datetime object.
        
        Args:
            date_text: Date string in MM/DD/YYYY format
            
        Returns:
            Optional[datetime]: Parsed date or None if invalid
        """
        if not date_text or date_text.strip() == "":
            return None
            
        try:
            # Handle MM/DD/YYYY format
            return datetime.strptime(date_text.strip(), "%m/%d/%Y")
        except ValueError:
            logger.warning(f"Could not parse date: {date_text}")
            return None
    
    def _parse_deadline(self, deadline_text: str) -> Optional[datetime]:
        """
        Parse deadline text, handling extensions.
        
        Args:
            deadline_text: Raw deadline text
            
        Returns:
            Optional[datetime]: Parsed deadline date
        """
        # Remove extension information and extract the actual deadline
        # Format: "05/12/2025<font color='green'><BR>Extended to 07/07/2025</font>"
        # or just "07/07/2025"
        
        # First, try to find extended date
        extended_match = re.search(r'Extended to (\d{2}/\d{2}/\d{4})', deadline_text)
        if extended_match:
            return self._parse_date(extended_match.group(1))
        
        # If no extension, clean up the text and parse the original date
        clean_text = re.sub(r'<[^>]+>', '', deadline_text)
        clean_text = clean_text.strip()
        
        return self._parse_date(clean_text)
    
    def _enhance_with_detail_page(self, opportunity: Opportunity, detail_url: str):
        """
        Enhance opportunity with information from the detail page.
        
        Args:
            opportunity: Opportunity object to enhance
            detail_url: URL of the detail page
        """
        try:
            response = self._make_sync_request(detail_url)
            if not response:
                return
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract additional information from detail page
            self._extract_detail_page_info(opportunity, soup)
            
        except Exception as e:
            logger.error(f"Error enhancing opportunity with detail page: {e}")
    
    def _extract_detail_page_info(self, opportunity: Opportunity, soup: BeautifulSoup):
        """
        Extract additional information from the detail page.
        
        Args:
            opportunity: Opportunity object to enhance
            soup: BeautifulSoup object of the detail page
        """
        try:
            # Look for additional description text
            description_elements = soup.find_all(["p", "div"], class_=re.compile(r"description|summary|overview", re.I))
            if description_elements:
                full_description = " ".join([elem.get_text(strip=True) for elem in description_elements])
                if full_description and full_description != opportunity.description_short:
                    opportunity.description_full = full_description
            
            # Look for contact information
            contact_elements = soup.find_all(["p", "div"], class_=re.compile(r"contact|phone|email", re.I))
            if contact_elements:
                contact_info = " ".join([elem.get_text(strip=True) for elem in contact_elements])
                if opportunity.contact_info is None:
                    opportunity.contact_info = {}
                opportunity.contact_info["additional_contact"] = contact_info
            
            # Look for document links
            document_links = soup.find_all("a", href=re.compile(r"\.pdf|\.doc|\.docx|\.xls|\.xlsx"))
            if document_links:
                documents = []
                for link in document_links:
                    doc_name = link.get_text(strip=True) or "Document"
                    doc_url = urljoin(self.base_url, link.get("href"))
                    documents.append(Document(
                        document_name=doc_name,
                        document_url=doc_url,
                        document_type="attachment"
                    ))
                opportunity.documents = documents
            
            # Look for WBE/HUB requirements
            wbe_elements = soup.find_all(string=re.compile(r"WBE|HUB|woman|minority|disadvantaged", re.I))
            if wbe_elements:
                if opportunity.contact_info is None:
                    opportunity.contact_info = {}
                opportunity.contact_info["wbe_requirements"] = " ".join(wbe_elements)
                
        except Exception as e:
            logger.error(f"Error extracting detail page info: {e}")
    
    def _create_standardized_opportunity(self, data: Dict[str, Any]) -> Opportunity:
        """
        Create a standardized opportunity object.
        
        Args:
            data: Dictionary containing opportunity data
            
        Returns:
            Opportunity: Standardized opportunity object
        """
        return Opportunity(
            title=data.get("title", ""),
            description_short=data.get("description_short", ""),
            description_full=data.get("description_full", ""),
            external_id=data.get("external_id", ""),
            issuing_entity_name=data.get("issuing_entity_name", ""),
            post_date=data.get("post_date"),
            due_date=data.get("due_date"),
            source_portal=data.get("source_portal", "SanAntonio"),
            opportunity_url=data.get("opportunity_url", ""),
            contact_info=data.get("contact_info", {}),
            documents=data.get("documents", [])
        ) 