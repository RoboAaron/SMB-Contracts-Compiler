"""
Scraping infrastructure for the Texas WBE Opportunity Discovery Engine.

This package provides the core scraping infrastructure including:
- Base scraper classes for portal-specific implementations
- HTTP client management with multiple client support
- Rate limiting and ethical scraping practices
- Data extraction utilities
- Error handling and retry logic
"""

from .base import BaseScraper
from .exceptions import ScrapingError, RateLimitError, RobotsTxtError
from .http_client import HTTPClientManager
from .rate_limiter import RateLimiter
from .extractors import ContentExtractor

__all__ = [
    "BaseScraper",
    "ScrapingError",
    "RateLimitError", 
    "RobotsTxtError",
    "HTTPClientManager",
    "RateLimiter",
    "ContentExtractor",
] 