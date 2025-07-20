"""
Enhanced Base Scraper

This module implements an enhanced base scraper that extends the original BaseScraper
with performance optimization capabilities, multi-mode extraction, advanced
configuration management, and AI analysis integration.
"""

import time
import logging
from typing import Dict, Any, Optional, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime

from bs4 import BeautifulSoup
import requests

from .base import BaseScraper
from .exceptions import ScrapingError, ParsingError

logger = logging.getLogger(__name__)

# AI analysis availability flag - will be set during initialization
AI_ANALYSIS_AVAILABLE = False


@dataclass
class PerformanceMetrics:
    """Data class for tracking performance metrics."""
    extraction_time: float
    memory_usage: float
    cpu_usage: float
    success_rate: float
    data_quality_score: float


@dataclass
class QualityMetrics:
    """Data class for tracking data quality metrics."""
    completeness: float
    accuracy: float
    consistency: float
    validity: float


class EnhancedBaseScraper(BaseScraper):
    """
    Enhanced Base Scraper with performance optimization and multi-mode extraction.
    
    This class extends the original BaseScraper with:
    - Multiple extraction modes (standardized, optimized, hybrid)
    - Performance optimization features (caching, parallel processing)
    - Enhanced configuration management
    - Portal-specific extensions
    - Performance and quality monitoring
    """
    
    def __init__(self, config: Union[Dict[str, Any], 'ScraperConfig'], portal_name: str, enable_ai_analysis: bool = True):
        """
        Initialize enhanced base scraper with configuration.
        
        Args:
            config: Portal-specific configuration (dict or ScraperConfig)
            portal_name: Name of the portal for tracking
            enable_ai_analysis: Whether to enable AI analysis integration
        """
        # Convert ScraperConfig to dict if needed
        if hasattr(config, 'model_dump'):
            # It's a Pydantic model
            config_dict = config.model_dump()
        else:
            # It's already a dict
            config_dict = config
        
        # Initialize base scraper
        super().__init__(config_dict, portal_name)
        
        # Enhanced configuration options
        self.extraction_mode = config_dict.get('extraction_mode', 'standardized')
        self.optimization_level = config_dict.get('optimization_level', 'balanced')
        self.performance_settings = config_dict.get('performance', {})
        self.field_mappings = config_dict.get('field_mappings', {})
        
        # AI Analysis integration - can be disabled
        self.enable_ai_analysis = enable_ai_analysis
        self.analysis_service = None
        
        # Only try to initialize AI analysis if explicitly enabled
        if self.enable_ai_analysis:
            try:
                # Lazy import of AI analysis components
                from ..ai_analysis.analysis_service import AnalysisService
                self.analysis_service = AnalysisService()
                logger.info(f"AI analysis enabled for {portal_name}")
            except Exception as e:
                logger.warning(f"Failed to initialize AI analysis for {portal_name}: {e}")
                self.enable_ai_analysis = False
        else:
            logger.info(f"AI analysis disabled for {portal_name}")
        
        # Performance tracking
        self.performance_metrics = {}
        self.quality_metrics = []
        self.start_time = None
        
        # Caching setup
        self._setup_caching()
        
        # Validate configuration
        self._validate_configuration()
        
        logger.info(f"Enhanced scraper initialized for {portal_name} with mode: {self.extraction_mode}")
    
    async def process_opportunities_with_analysis(self, opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process scraped opportunities with AI analysis integration.
        
        Args:
            opportunities: List of scraped opportunity data dictionaries
            
        Returns:
            List of opportunities with analysis results attached
        """
        if not self.enable_ai_analysis or not opportunities:
            return opportunities
        
        processed_opportunities = []
        
        for opp_data in opportunities:
            try:
                # Convert to Opportunity object if needed
                if isinstance(opp_data, dict):
                    # Create temporary opportunity object for analysis
                    from ..database.models import Opportunity
                    opportunity = Opportunity(**opp_data)
                else:
                    opportunity = opp_data
                
                # Trigger AI analysis if opportunity has been saved to database
                if hasattr(opportunity, 'id') and opportunity.id:
                    analysis_result = await self.analysis_service.analyze_opportunity(opportunity.id)
                    
                    # Attach analysis results to opportunity data
                    if isinstance(opp_data, dict):
                        opp_data['analysis_result'] = {
                            'relevance_score': float(analysis_result.relevance_score) if analysis_result.relevance_score else None,
                            'advantage_score': float(analysis_result.advantage_score) if analysis_result.advantage_score else None,
                            'is_relevant': analysis_result.is_relevant,
                            'primary_category': analysis_result.primary_category,
                            'advantage_type': analysis_result.advantage_type,
                            'final_briefing_summary': analysis_result.final_briefing_summary,
                            'actionable_insight': analysis_result.actionable_insight
                        }
                    
                    logger.info(f"AI analysis completed for opportunity {opportunity.external_id}")
                
                processed_opportunities.append(opp_data)
                
            except Exception as e:
                logger.error(f"Error processing opportunity with AI analysis: {e}")
                # Add opportunity without analysis
                processed_opportunities.append(opp_data)
        
        return processed_opportunities
    
    async def analyze_opportunity_batch(self, opportunity_ids: List[str]) -> Dict[str, Any]:
        """
        Trigger batch AI analysis for a list of opportunity IDs.
        
        Args:
            opportunity_ids: List of opportunity external IDs or UUIDs
            
        Returns:
            Dictionary with analysis results and statistics
        """
        if not self.enable_ai_analysis:
            logger.warning("AI analysis not enabled - skipping batch analysis")
            return {"status": "skipped", "reason": "AI analysis not enabled"}
        
        try:
            # Convert external IDs to UUIDs if needed
            from uuid import UUID
            from ..database.models import Opportunity
            from ..database.connection import get_async_session
            from sqlalchemy import select
            
            uuids = []
            async with get_async_session() as session:
                for opp_id in opportunity_ids:
                    try:
                        # Try to parse as UUID first
                        uuid_obj = UUID(opp_id)
                        uuids.append(uuid_obj)
                    except ValueError:
                        # It's an external ID, look up the UUID
                        stmt = select(Opportunity.id).where(Opportunity.external_id == opp_id)
                        result = await session.execute(stmt)
                        uuid_obj = result.scalar_one_or_none()
                        if uuid_obj:
                            uuids.append(uuid_obj)
            
            # Perform batch analysis
            analysis_results = await self.analysis_service.analyze_opportunities_batch(uuids)
            
            return {
                "status": "completed",
                "total_requested": len(opportunity_ids),
                "total_analyzed": len(analysis_results),
                "analysis_results": analysis_results
            }
            
        except Exception as e:
            logger.error(f"Error in batch AI analysis: {e}")
            return {"status": "error", "error": str(e)}

    def extract_opportunity_data(self, element) -> Optional[Dict[str, Any]]:
        """
        Extract opportunity data using the configured extraction mode.
        
        Args:
            element: BeautifulSoup element containing opportunity data
            
        Returns:
            dict: Standardized opportunity data or None if extraction fails
        """
        start_time = time.time()
        
        try:
            # Route to appropriate extraction method based on mode
            if self.extraction_mode == 'optimized':
                data = self._extract_optimized(element)
            elif self.extraction_mode == 'hybrid':
                data = self._extract_hybrid(element)
            else:  # standardized
                data = self.extract_standardized_opportunity_data(element, self.portal_name)
            
            # Track performance
            self._track_performance('extract_opportunity_data', start_time)
            
            # Track quality if data was extracted
            if data:
                self._track_data_quality(data)
            
            return data
            
        except Exception as e:
            logger.error(f"Error in {self.extraction_mode} extraction: {e}")
            # Try fallback extraction
            return self._extract_with_fallback(element)
    
    def _extract_optimized(self, element) -> Optional[Dict[str, Any]]:
        """
        Extract data using portal-specific optimizations for maximum performance.
        
        Args:
            element: BeautifulSoup element containing opportunity data
            
        Returns:
            dict: Extracted data using optimized methods
        """
        start_time = time.time()
        
        try:
            # Use direct selector-based extraction for speed
            data = {}
            
            # Apply custom field mappings first
            data = self._apply_custom_mappings(data, element)
            
            # Use fallback mappings for missing fields
            data = self._apply_fallback_mappings(data, element)
            
            # Track performance
            self._track_performance('_extract_optimized', start_time)
            
            return data if data else None
            
        except Exception as e:
            logger.error(f"Error in optimized extraction: {e}")
            return None
    
    def _extract_hybrid(self, element) -> Optional[Dict[str, Any]]:
        """
        Extract data using standardized methods for common fields and
        portal-specific methods for complex fields.
        
        Args:
            element: BeautifulSoup element containing opportunity data
            
        Returns:
            dict: Extracted data using hybrid approach
        """
        start_time = time.time()
        
        try:
            # Start with standardized extraction for common fields
            data = self.extract_standardized_opportunity_data(element, self.portal_name)
            
            if not data:
                data = {}
            
            # Add portal-specific optimizations for complex fields
            if not data.get('estimated_value'):
                data['estimated_value'] = self._extract_value_optimized(element)
            
            if not data.get('contact_info'):
                data['contact_info'] = self._extract_contact_optimized(element)
            
            # Apply custom field mappings
            data = self._apply_custom_mappings(data, element)
            
            # Track performance
            self._track_performance('_extract_hybrid', start_time)
            
            return data if data else None
            
        except Exception as e:
            logger.error(f"Error in hybrid extraction: {e}")
            return None
    
    def _extract_with_fallback(self, element) -> Optional[Dict[str, Any]]:
        """
        Extract data with multiple fallback strategies.
        
        Args:
            element: BeautifulSoup element containing opportunity data
            
        Returns:
            dict: Extracted data or None if all methods fail
        """
        fallback_methods = [
            ('standardized', lambda: self.extract_standardized_opportunity_data(element, self.portal_name)),
            ('optimized', lambda: self._extract_optimized(element)),
            ('portal_specific', lambda: self._extract_portal_specific_fallback(element))
        ]
        
        for method_name, method_func in fallback_methods:
            try:
                logger.debug(f"Trying fallback method: {method_name}")
                result = method_func()
                if result:
                    logger.info(f"Fallback method {method_name} succeeded")
                    return result
            except Exception as e:
                logger.warning(f"Fallback method {method_name} failed: {e}")
                continue
        
        logger.error("All extraction methods failed")
        return None
    
    def _apply_custom_mappings(self, data: Dict[str, Any], element) -> Dict[str, Any]:
        """
        Apply portal-specific field mappings and transformations.
        
        Args:
            data: Current extracted data
            element: BeautifulSoup element
            
        Returns:
            dict: Updated data with custom mappings applied
        """
        custom_mappings = self.field_mappings.get('custom_fields', [])
        
        for mapping in custom_mappings:
            try:
                source = mapping['source']
                target = mapping['target']
                transform = mapping.get('transform')
                
                # Extract source field
                source_value = self._extract_field(element, source)
                if source_value:
                    # Apply transformation
                    if transform == 'uppercase':
                        source_value = source_value.upper()
                    elif transform == 'currency':
                        source_value = self._parse_currency(source_value)
                    elif transform == 'date':
                        source_value = self._parse_date(source_value)
                    
                    data[target] = source_value
                    
            except Exception as e:
                logger.debug(f"Error applying custom mapping {mapping}: {e}")
                continue
        
        return data
    
    def _apply_fallback_mappings(self, data: Dict[str, Any], element) -> Dict[str, Any]:
        """
        Apply fallback field mappings for missing data.
        
        Args:
            data: Current extracted data
            element: BeautifulSoup element
            
        Returns:
            dict: Updated data with fallback mappings applied
        """
        fallback_mappings = self.field_mappings.get('fallback_mappings', {})
        
        for target_field, source_fields in fallback_mappings.items():
            if not data.get(target_field):
                for source_field in source_fields:
                    try:
                        value = self._extract_field(element, source_field)
                        if value:
                            data[target_field] = value
                            break
                    except Exception as e:
                        logger.debug(f"Error extracting fallback field {source_field}: {e}")
                        continue
        
        return data
    
    def _extract_field(self, element, field_name: str) -> Optional[str]:
        """
        Extract a specific field from the element.
        
        Args:
            element: BeautifulSoup element
            field_name: Name of the field to extract
            
        Returns:
            str: Extracted field value or None
        """
        # Try different extraction strategies
        strategies = [
            lambda: self._extract_by_selector(element, field_name),
            lambda: self._extract_by_text_search(element, field_name),
            lambda: self._extract_by_attribute(element, field_name)
        ]
        
        for strategy in strategies:
            try:
                result = strategy()
                if result:
                    return result
            except Exception as e:
                logger.debug(f"Strategy failed for field {field_name}: {e}")
                continue
        
        return None
    
    def _extract_by_selector(self, element, field_name: str) -> Optional[str]:
        """Extract field using CSS selector."""
        selectors = self.config.get('selectors', {})
        selector = selectors.get(field_name)
        
        if selector:
            found_element = element.select_one(selector)
            if found_element:
                return found_element.get_text(strip=True)
        
        return None
    
    def _extract_by_text_search(self, element, field_name: str) -> Optional[str]:
        """Extract field by searching for text patterns."""
        field_patterns = {
            'title': ['title', 'description', 'project'],
            'external_id': ['bid', 'reference', 'number', 'id'],
            'issuing_entity_name': ['agency', 'department', 'entity'],
            'due_date': ['due', 'deadline', 'closing', 'date']
        }
        patterns = field_patterns.get(field_name, [])
        for pattern in patterns:
            found = self._extract_text_around_pattern(element, pattern)
            if found:
                return found
        return None

    def _extract_by_attribute(self, element, field_name: str) -> Optional[str]:
        """Extract field from element attributes."""
        # If data-field matches field_name, return data-name if present
        if element.get('data-field', '').lower() == field_name.lower():
            data_name = element.get('data-name')
            if data_name:
                return data_name
        # Fallback: check common attributes
        for attr in ['data-field', 'data-name', 'aria-label', 'title']:
            value = element.get(attr)
            if value and field_name.lower() in value.lower():
                return value
        return None
    
    def _extract_text_around_pattern(self, element, pattern: str) -> Optional[str]:
        """Extract text around a specific pattern."""
        # Simplified implementation - in practice, you'd use more sophisticated text analysis
        text = element.get_text()
        pattern_index = text.lower().find(pattern.lower())
        
        if pattern_index != -1:
            # Extract text around the pattern
            start = max(0, pattern_index - 50)
            end = min(len(text), pattern_index + 100)
            return text[start:end].strip()
        
        return None
    
    def _extract_value_optimized(self, element) -> Optional[float]:
        """Extract estimated value using optimized methods."""
        # Portal-specific implementation
        # This should be overridden by portal scrapers
        return None
    
    def _extract_contact_optimized(self, element) -> Optional[Dict[str, Any]]:
        """Extract contact information using optimized methods."""
        # Portal-specific implementation
        # This should be overridden by portal scrapers
        return None
    
    def _extract_portal_specific_fallback(self, element) -> Optional[Dict[str, Any]]:
        """Portal-specific fallback extraction method."""
        # This should be overridden by portal scrapers
        return None
    
    def _setup_caching(self):
        """Setup response caching for improved performance."""
        if self.performance_settings.get('cache_responses'):
            self.cache = {}
            self.cache_ttl = self.performance_settings.get('cache_ttl', 300)  # 5 minutes
            logger.debug("Response caching enabled")
    
    def _get_cached_response(self, url: str) -> Optional[requests.Response]:
        """Get cached response if available and not expired."""
        if hasattr(self, 'cache') and url in self.cache:
            timestamp, response = self.cache[url]
            if time.time() - timestamp < self.cache_ttl:
                logger.debug(f"Using cached response for {url}")
                return response
        return None
    
    def _cache_response(self, url: str, response: requests.Response):
        """Cache response for future use."""
        if hasattr(self, 'cache'):
            self.cache[url] = (time.time(), response)
            logger.debug(f"Cached response for {url}")
    
    def scrape_opportunities_parallel(self, elements: List[BeautifulSoup]) -> List[Dict[str, Any]]:
        """
        Scrape opportunities in parallel for improved performance.
        
        Args:
            elements: List of BeautifulSoup elements containing opportunity data
            
        Returns:
            list: List of extracted opportunity data
        """
        if not self.performance_settings.get('parallel_processing'):
            return [self.extract_opportunity_data(elem) for elem in elements]
        
        max_workers = self.performance_settings.get('max_workers', 4)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.extract_opportunity_data, elem) 
                      for elem in elements]
            
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error in parallel extraction: {e}")
                    continue
        
        return results
    
    def _track_performance(self, method_name: str, start_time: float):
        """Track extraction performance for optimization."""
        execution_time = time.time() - start_time
        
        if method_name not in self.performance_metrics:
            self.performance_metrics[method_name] = []
        
        self.performance_metrics[method_name].append(execution_time)
        
        # Log performance if it exceeds thresholds
        if len(self.performance_metrics[method_name]) > 1:
            avg_time = sum(self.performance_metrics[method_name]) / len(self.performance_metrics[method_name])
            if execution_time > avg_time * 2:
                logger.warning(f"Slow extraction detected: {method_name} took {execution_time:.2f}s")
    
    def _track_data_quality(self, data: Dict[str, Any]):
        """Track data quality metrics for optimization."""
        quality_metrics = self._calculate_quality_metrics(data)
        
        # Log quality issues
        if quality_metrics.completeness < 0.8:
            logger.warning(f"Low data completeness: {quality_metrics.completeness:.2f}")
        
        # Store metrics for analysis
        if not hasattr(self, 'quality_metrics'):
            self.quality_metrics = []
        
        self.quality_metrics.append(quality_metrics)
    
    def _calculate_quality_metrics(self, data: Dict[str, Any]) -> QualityMetrics:
        """Calculate data quality metrics."""
        required_fields = ['title', 'external_id', 'source_portal']
        optional_fields = ['description_short', 'issuing_entity_name', 'due_date', 'status']
        
        # Calculate completeness
        required_completeness = sum(1 for field in required_fields if data.get(field)) / len(required_fields)
        optional_completeness = sum(1 for field in optional_fields if data.get(field)) / len(optional_fields)
        completeness = (required_completeness * 0.7) + (optional_completeness * 0.3)
        
        # Calculate accuracy (simplified - in practice, you'd use more sophisticated validation)
        accuracy = 1.0  # Placeholder
        
        # Calculate consistency (simplified)
        consistency = 1.0  # Placeholder
        
        # Calculate validity (simplified)
        validity = 1.0  # Placeholder
        
        return QualityMetrics(
            completeness=completeness,
            accuracy=accuracy,
            consistency=consistency,
            validity=validity
        )
    
    def _validate_configuration(self):
        """Validate enhanced configuration."""
        valid_modes = ['standardized', 'optimized', 'hybrid', 'api_first', 'html_only', 'selenium_only']
        valid_levels = ['balanced', 'speed', 'quality']
        
        if self.extraction_mode not in valid_modes:
            logger.warning(f"Invalid extraction mode: {self.extraction_mode}. Using 'standardized'")
            self.extraction_mode = 'standardized'
        
        if self.optimization_level not in valid_levels:
            logger.warning(f"Invalid optimization level: {self.optimization_level}. Using 'balanced'")
            self.optimization_level = 'balanced'
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for monitoring."""
        summary = {
            'portal_name': self.portal_name,
            'extraction_mode': self.extraction_mode,
            'optimization_level': self.optimization_level,
            'performance_metrics': {},
            'quality_metrics': {}
        }
        
        # Calculate average performance metrics
        for method_name, times in self.performance_metrics.items():
            if times:
                summary['performance_metrics'][method_name] = {
                    'avg_time': sum(times) / len(times),
                    'min_time': min(times),
                    'max_time': max(times),
                    'count': len(times)
                }
        
        # Calculate average quality metrics
        if hasattr(self, 'quality_metrics') and self.quality_metrics:
            avg_completeness = sum(q.completeness for q in self.quality_metrics) / len(self.quality_metrics)
            avg_accuracy = sum(q.accuracy for q in self.quality_metrics) / len(self.quality_metrics)
            
            summary['quality_metrics'] = {
                'avg_completeness': avg_completeness,
                'avg_accuracy': avg_accuracy,
                'total_records': len(self.quality_metrics)
            }
        
        return summary
    
    def reset_metrics(self):
        """Reset performance and quality metrics."""
        self.performance_metrics = {}
        if hasattr(self, 'quality_metrics'):
            self.quality_metrics = []
        logger.info("Performance and quality metrics reset")
    
    def scrape_opportunities(self, max_pages: int = 1) -> List[Dict[str, Any]]:
        """
        Scrape opportunities using enhanced architecture.
        
        This is a base implementation that should be overridden by portal-specific scrapers.
        
        Args:
            max_pages: Maximum number of pages to scrape
            
        Returns:
            List of extracted opportunity data dictionaries
        """
        logger.warning("Base scrape_opportunities method called - should be overridden by portal scrapers")
        return []
    
    def _parse_currency(self, value_text: str) -> Optional[float]:
        """
        Parse currency value from text.
        
        Args:
            value_text: Text containing currency value
            
        Returns:
            float: Parsed currency value or None
        """
        try:
            # Remove common currency symbols and whitespace
            cleaned = value_text.replace('$', '').replace(',', '').replace(' ', '')
            
            # Extract numeric value
            import re
            match = re.search(r'[\d,]+\.?\d*', cleaned)
            if match:
                return float(match.group(0).replace(',', ''))
            
            return None
        except Exception as e:
            logger.debug(f"Error parsing currency '{value_text}': {e}")
            return None
    
    def _parse_date(self, date_text: str) -> Optional[datetime]:
        """
        Parse date from text.
        
        Args:
            date_text: Text containing date
            
        Returns:
            datetime: Parsed date or None
        """
        try:
            # Try common date formats
            from dateutil import parser
            
            # Clean up the text
            cleaned = date_text.strip()
            
            # Parse with dateutil
            return parser.parse(cleaned)
        except Exception as e:
            logger.debug(f"Error parsing date '{date_text}': {e}")
            return None 