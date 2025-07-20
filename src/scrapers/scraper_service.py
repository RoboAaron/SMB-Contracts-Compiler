#!/usr/bin/env python3
"""
Scraper Service - Unified Interface for All Portal Scrapers

This service provides a unified interface for coordinating all portal scrapers
(ESBD, BeaconBid/Houston, San Antonio) and integrating them with the master dashboard.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from ..config import Config
from .esbd import ESBDScraper
from .beaconbid import BeaconBidScraper
from .san_antonio import SanAntonioScraper
from .exceptions import ScrapingError
from ..database.models import Opportunity, ScrapingLog
from ..database.connection import get_db

logger = logging.getLogger(__name__)

@dataclass
class ScrapingResult:
    """Result of a scraping operation."""
    portal: str
    success: bool
    opportunities_found: int
    error_message: Optional[str] = None
    execution_time: float = 0.0
    opportunities: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.opportunities is None:
            self.opportunities = []

@dataclass
class ScrapingProgress:
    """Progress information for ongoing scraping operations."""
    portal: str
    status: str  # 'starting', 'running', 'completed', 'failed'
    progress_percentage: int = 0
    current_page: int = 0
    total_pages: int = 0
    opportunities_found: int = 0
    message: str = ""

class ScraperService:
    """
    Unified service for coordinating all portal scrapers.
    
    Provides a single interface for the master dashboard to:
    - Run individual scrapers
    - Run all scrapers in parallel
    - Monitor scraping progress
    - Handle errors and retries
    - Store results in database
    """
    
    def __init__(self, config: Config):
        """Initialize the scraper service."""
        self.config = config
        self.scrapers = {}
        self.progress_callbacks: List[Callable[[ScrapingProgress], None]] = []
        self.active_tasks = {}
        
        # Initialize scrapers
        self._initialize_scrapers()
        
    def _initialize_scrapers(self):
        """Initialize all available scrapers."""
        try:
            # Initialize ESBD scraper
            if hasattr(self.config, 'scrapers') and 'esbd' in self.config.scrapers:
                self.scrapers['esbd'] = ESBDScraper(self.config)
                logger.info("ESBD scraper initialized")
            
            # Initialize BeaconBid (Houston) scraper
            if hasattr(self.config, 'scrapers') and 'beaconbid' in self.config.scrapers:
                # Convert ScraperConfig to dict for BeaconBid scraper
                beaconbid_config = self.config.scrapers['beaconbid']
                if hasattr(beaconbid_config, 'model_dump'):
                    beaconbid_dict = beaconbid_config.model_dump()
                else:
                    beaconbid_dict = beaconbid_config.__dict__
                self.scrapers['houston'] = BeaconBidScraper(beaconbid_dict)
                logger.info("BeaconBid (Houston) scraper initialized")
            
            # Initialize San Antonio scraper
            if hasattr(self.config, 'scrapers') and 'san_antonio' in self.config.scrapers:
                # Convert ScraperConfig to dict for San Antonio scraper
                san_antonio_config = self.config.scrapers['san_antonio']
                if hasattr(san_antonio_config, 'model_dump'):
                    san_antonio_dict = san_antonio_config.model_dump()
                else:
                    san_antonio_dict = san_antonio_config.__dict__
                self.scrapers['san_antonio'] = SanAntonioScraper(san_antonio_dict)
                logger.info("San Antonio scraper initialized")
                
            logger.info(f"Scraper service initialized with {len(self.scrapers)} scrapers")
            
        except Exception as e:
            logger.error(f"Error initializing scrapers: {e}")
            # Continue with available scrapers
    
    def add_progress_callback(self, callback: Callable[[ScrapingProgress], None]):
        """Add a callback function to receive progress updates."""
        self.progress_callbacks.append(callback)
    
    def remove_progress_callback(self, callback: Callable[[ScrapingProgress], None]):
        """Remove a progress callback."""
        if callback in self.progress_callbacks:
            self.progress_callbacks.remove(callback)
    
    def _notify_progress(self, progress: ScrapingProgress):
        """Notify all registered callbacks of progress updates."""
        for callback in self.progress_callbacks:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Error in progress callback: {e}")
    
    async def scrape_portal(self, portal_name: str, max_opportunities: int = 50) -> ScrapingResult:
        """
        Scrape a specific portal.
        
        Args:
            portal_name: Name of the portal ('esbd', 'houston', 'san_antonio')
            max_opportunities: Maximum number of opportunities to scrape
            
        Returns:
            ScrapingResult object with results
        """
        start_time = time.time()
        
        if portal_name not in self.scrapers:
            error_msg = f"Scraper for portal '{portal_name}' not available"
            logger.error(error_msg)
            return ScrapingResult(
                portal=portal_name,
                success=False,
                opportunities_found=0,
                error_message=error_msg,
                execution_time=time.time() - start_time
            )
        
        try:
            # Notify start
            self._notify_progress(ScrapingProgress(
                portal=portal_name,
                status='starting',
                message=f"Starting {portal_name} scraper..."
            ))
            
            # Run scraper in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                # Update progress
                self._notify_progress(ScrapingProgress(
                    portal=portal_name,
                    status='running',
                    progress_percentage=25,
                    message=f"Scraping {portal_name} opportunities..."
                ))
                
                # Create a wrapper function that recreates the scraper in thread context
                def scrape_in_thread():
                    """Wrapper function to recreate scraper in thread context to avoid serialization issues."""
                    try:
                        # Recreate the scraper in the thread context
                        if portal_name == 'esbd':
                            from .esbd import ESBDScraper
                            scraper = ESBDScraper(self.config)
                            return scraper.scrape_opportunities(5)  # max_pages for ESBD
                        elif portal_name == 'houston':
                            from .beaconbid import BeaconBidScraper
                            # Convert ScraperConfig to dict for BeaconBid scraper
                            beaconbid_config = self.config.scrapers['beaconbid']
                            if hasattr(beaconbid_config, 'model_dump'):
                                beaconbid_dict = beaconbid_config.model_dump()
                            else:
                                beaconbid_dict = beaconbid_config.__dict__
                            scraper = BeaconBidScraper(beaconbid_dict)
                            return scraper.scrape_opportunities()
                        elif portal_name == 'san_antonio':
                            from .san_antonio import SanAntonioScraper
                            # Convert ScraperConfig to dict for San Antonio scraper
                            san_antonio_config = self.config.scrapers['san_antonio']
                            if hasattr(san_antonio_config, 'model_dump'):
                                san_antonio_dict = san_antonio_config.model_dump()
                            else:
                                san_antonio_dict = san_antonio_config.__dict__
                            scraper = SanAntonioScraper(san_antonio_dict)
                            return scraper.scrape_opportunities()
                        else:
                            raise ScrapingError(f"Unknown portal: {portal_name}")
                    except Exception as e:
                        logger.error(f"Error in thread scraper execution: {e}")
                        raise
                
                # Execute scraping using the wrapper function
                opportunities = await loop.run_in_executor(executor, scrape_in_thread)
                
                # Convert opportunities to standard format
                standardized_opportunities = []
                for opp in opportunities:
                    if isinstance(opp, dict):
                        standardized_opportunities.append(opp)
                    else:
                        # Convert database model to dict
                        standardized_opportunities.append(self._convert_opportunity_to_dict(opp))
                
                # Update progress
                self._notify_progress(ScrapingProgress(
                    portal=portal_name,
                    status='running',
                    progress_percentage=75,
                    opportunities_found=len(standardized_opportunities),
                    message=f"Processing {len(standardized_opportunities)} opportunities..."
                ))
                
                # Store in database if needed
                await self._store_opportunities(portal_name, standardized_opportunities)
                
                execution_time = time.time() - start_time
                
                # Notify completion
                self._notify_progress(ScrapingProgress(
                    portal=portal_name,
                    status='completed',
                    progress_percentage=100,
                    opportunities_found=len(standardized_opportunities),
                    message=f"Completed: {len(standardized_opportunities)} opportunities found"
                ))
                
                logger.info(f"Successfully scraped {len(standardized_opportunities)} opportunities from {portal_name}")
                
                return ScrapingResult(
                    portal=portal_name,
                    success=True,
                    opportunities_found=len(standardized_opportunities),
                    execution_time=execution_time,
                    opportunities=standardized_opportunities
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Error scraping {portal_name}: {str(e)}"
            logger.error(error_msg)
            
            # Notify failure
            self._notify_progress(ScrapingProgress(
                portal=portal_name,
                status='failed',
                message=error_msg
            ))
            
            return ScrapingResult(
                portal=portal_name,
                success=False,
                opportunities_found=0,
                error_message=error_msg,
                execution_time=execution_time
            )
    
    async def scrape_all_portals(self, max_opportunities_per_portal: int = 50) -> List[ScrapingResult]:
        """
        Scrape all available portals in parallel.
        
        Args:
            max_opportunities_per_portal: Maximum opportunities per portal
            
        Returns:
            List of ScrapingResult objects
        """
        logger.info(f"Starting parallel scraping of {len(self.scrapers)} portals")
        
        # Create tasks for all portals
        tasks = []
        for portal_name in self.scrapers.keys():
            task = asyncio.create_task(
                self.scrape_portal(portal_name, max_opportunities_per_portal)
            )
            tasks.append(task)
            self.active_tasks[portal_name] = task
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        scraping_results = []
        for i, result in enumerate(results):
            portal_name = list(self.scrapers.keys())[i]
            
            if isinstance(result, Exception):
                logger.error(f"Exception in {portal_name} scraping: {result}")
                scraping_results.append(ScrapingResult(
                    portal=portal_name,
                    success=False,
                    opportunities_found=0,
                    error_message=str(result)
                ))
            else:
                scraping_results.append(result)
            
            # Clean up task reference
            if portal_name in self.active_tasks:
                del self.active_tasks[portal_name]
        
        # Log summary
        total_opportunities = sum(r.opportunities_found for r in scraping_results)
        successful_portals = sum(1 for r in scraping_results if r.success)
        
        logger.info(f"Parallel scraping completed: {successful_portals}/{len(scraping_results)} portals successful, {total_opportunities} total opportunities")
        
        return scraping_results
    
    def stop_scraping(self, portal_name: Optional[str] = None):
        """
        Stop ongoing scraping operations.
        
        Args:
            portal_name: Specific portal to stop, or None to stop all
        """
        if portal_name:
            if portal_name in self.active_tasks:
                self.active_tasks[portal_name].cancel()
                del self.active_tasks[portal_name]
                logger.info(f"Stopped scraping for {portal_name}")
        else:
            # Stop all active tasks
            for portal, task in self.active_tasks.items():
                task.cancel()
                logger.info(f"Stopped scraping for {portal}")
            self.active_tasks.clear()
    
    def get_scraping_status(self) -> Dict[str, Any]:
        """
        Get current scraping status.
        
        Returns:
            Dictionary with scraping status information
        """
        return {
            'available_portals': list(self.scrapers.keys()),
            'active_tasks': list(self.active_tasks.keys()),
            'total_scrapers': len(self.scrapers),
            'active_scrapers': len(self.active_tasks)
        }
    
    def _convert_opportunity_to_dict(self, opportunity: Any) -> Dict[str, Any]:
        """Convert opportunity object to dictionary format."""
        if isinstance(opportunity, dict):
            return opportunity
        
        # Handle database model objects
        if hasattr(opportunity, '__dict__'):
            result = {}
            for key, value in opportunity.__dict__.items():
                if not key.startswith('_'):
                    if isinstance(value, datetime):
                        result[key] = value.isoformat()
                    else:
                        result[key] = value
            return result
        
        # Fallback for other types
        return {
            'title': str(getattr(opportunity, 'title', 'Unknown')),
            'description': str(getattr(opportunity, 'description', '')),
            'portal': str(getattr(opportunity, 'portal', 'Unknown')),
            'scraped_at': datetime.now().isoformat()
        }
    
    async def _store_opportunities(self, portal_name: str, opportunities: List[Dict[str, Any]]):
        """Store scraped opportunities in database."""
        try:
            # Import and initialize opportunity repository
            from ..database.repositories.opportunity_repository import OpportunityRepository
            
            repo = OpportunityRepository()
            stored_count = repo.store_opportunities(opportunities, portal_name)
            
            logger.info(f"Stored {stored_count} opportunities from {portal_name}")
            
        except Exception as e:
            logger.error(f"Error storing opportunities from {portal_name}: {e}")
            # Continue without storing - don't fail the entire scraping process
    
    def test_scrapers(self) -> Dict[str, bool]:
        """
        Test all scrapers to verify they're working.
        
        Returns:
            Dictionary mapping portal names to test results
        """
        results = {}
        
        for portal_name, scraper in self.scrapers.items():
            try:
                # Test basic functionality
                if hasattr(scraper, 'base_url'):
                    # Simple connectivity test
                    results[portal_name] = True
                    logger.info(f"Scraper {portal_name} test passed")
                else:
                    results[portal_name] = False
                    logger.warning(f"Scraper {portal_name} missing base_url")
            except Exception as e:
                results[portal_name] = False
                logger.error(f"Scraper {portal_name} test failed: {e}")
        
        return results
    
    def get_portal_info(self, portal_name: str) -> Dict[str, Any]:
        """Get information about a specific portal."""
        if portal_name not in self.scrapers:
            return {'error': f'Portal {portal_name} not available'}
        
        scraper = self.scrapers[portal_name]
        
        return {
            'name': portal_name,
            'base_url': getattr(scraper, 'base_url', 'Unknown'),
            'status': 'available',
            'last_run': None,  # TODO: Get from database
            'total_opportunities': 0  # TODO: Get from database
        }
    
    def close(self):
        """Clean up all scrapers and resources."""
        # Stop any active tasks
        self.stop_scraping()
        
        # Close individual scrapers
        for portal_name, scraper in self.scrapers.items():
            try:
                if hasattr(scraper, 'close'):
                    scraper.close()
                logger.info(f"Closed scraper for {portal_name}")
            except Exception as e:
                logger.error(f"Error closing scraper {portal_name}: {e}")
        
        self.scrapers.clear()
        self.progress_callbacks.clear()
        logger.info("Scraper service closed") 