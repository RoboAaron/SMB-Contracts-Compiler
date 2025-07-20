"""
Rate limiting and ethical scraping utilities.

Provides robots.txt compliance, rate limiting, and respectful crawling practices.
"""

import asyncio
import time
from datetime import datetime, time as dt_time
from typing import Dict, Optional, Set
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

from ..config import ScrapingConfig
from .exceptions import RateLimitError, RobotsTxtError, ScrapingError


class RateLimiter:
    """Manages rate limiting and ethical scraping practices."""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.delays: Dict[str, float] = {}
        self.robots_cache: Dict[str, Optional[RobotFileParser]] = {}
        self.last_request_time: Dict[str, float] = {}
        self.disallowed_paths: Dict[str, Set[str]] = {}
    
    async def check_robots_txt(self, url: str) -> bool:
        """
        Check robots.txt for the given URL.
        
        Args:
            url: URL to check robots.txt for
            
        Returns:
            True if allowed, False if disallowed
            
        Raises:
            RobotsTxtError: If robots.txt parsing fails
        """
        if not self.config.respect_robots_txt:
            return True
        
        domain = urlparse(url).netloc
        path = urlparse(url).path
        
        # Check cache first
        if domain in self.robots_cache:
            robots_parser = self.robots_cache[domain]
            if robots_parser is None:
                return True  # No robots.txt found, assume allowed
            
            # Check if path is disallowed
            if domain in self.disallowed_paths and path in self.disallowed_paths[domain]:
                return False
            
            return robots_parser.can_fetch(self.config.user_agent, url)
        
        # Fetch and parse robots.txt
        try:
            robots_url = urljoin(f"https://{domain}", "/robots.txt")
            robots_parser = RobotFileParser()
            robots_parser.set_url(robots_url)
            
            # Try to read robots.txt
            try:
                robots_parser.read()
                self.robots_cache[domain] = robots_parser
                
                # Cache disallowed paths for this domain
                disallowed = set()
                for rule in robots_parser.site_maps():
                    if not robots_parser.can_fetch(self.config.user_agent, rule):
                        disallowed.add(urlparse(rule).path)
                
                self.disallowed_paths[domain] = disallowed
                
                return robots_parser.can_fetch(self.config.user_agent, url)
                
            except Exception:
                # If robots.txt is not accessible, assume allowed
                self.robots_cache[domain] = None
                return True
                
        except Exception as e:
            raise RobotsTxtError(f"Failed to check robots.txt for {domain}: {e}", robots_url)
    
    async def respect_rate_limit(self, url: str) -> float:
        """
        Apply rate limiting for the given URL.
        
        Args:
            url: URL to apply rate limiting for
            
        Returns:
            Delay applied in seconds
        """
        domain = urlparse(url).netloc
        current_time = time.time()
        
        # Check if we need to wait
        if domain in self.last_request_time:
            time_since_last = current_time - self.last_request_time[domain]
            required_delay = self._get_required_delay(domain)
            
            if time_since_last < required_delay:
                delay_needed = required_delay - time_since_last
                await asyncio.sleep(delay_needed)
                return delay_needed
        
        # Update last request time
        self.last_request_time[domain] = time.time()
        return 0.0
    
    def _get_required_delay(self, domain: str) -> float:
        """Get the required delay for a domain."""
        # Check if we're in off-peak hours
        if self._is_off_peak_hours():
            return self.config.request_delay * 0.5  # Faster during off-peak
        
        # Check domain-specific delays
        if domain in self.delays:
            return self.delays[domain]
        
        return self.config.request_delay
    
    def _is_off_peak_hours(self) -> bool:
        """Check if current time is within off-peak hours."""
        current_time = datetime.now().time()
        start_time_str = self.config.off_peak_hours["start"]
        end_time_str = self.config.off_peak_hours["end"]
        
        # Parse times
        start_time = datetime.strptime(start_time_str, "%H:%M").time()
        end_time = datetime.strptime(end_time_str, "%H:%M").time()
        
        # Handle overnight periods (e.g., 23:00 to 06:00)
        if start_time > end_time:
            return current_time >= start_time or current_time <= end_time
        else:
            return start_time <= current_time <= end_time
    
    def set_domain_delay(self, domain: str, delay: float):
        """Set a custom delay for a specific domain."""
        self.delays[domain] = delay
    
    def get_domain_delay(self, domain: str) -> float:
        """Get the current delay for a domain."""
        return self.delays.get(domain, self.config.request_delay)
    
    def wait(self, url: str = "default"):
        """Synchronous wait method for rate limiting."""
        import time
        domain = urlparse(url).netloc if url != "default" else "default"
        current_time = time.time()
        
        # Check if we need to wait
        if domain in self.last_request_time:
            time_since_last = current_time - self.last_request_time[domain]
            required_delay = self._get_required_delay(domain)
            
            if time_since_last < required_delay:
                delay_needed = required_delay - time_since_last
                time.sleep(delay_needed)
                return delay_needed
        
        # Update last request time
        self.last_request_time[domain] = time.time()
        return 0.0
    
    def is_allowed_by_robots_txt(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt (synchronous version)."""
        if not self.config.respect_robots_txt:
            return True
        
        domain = urlparse(url).netloc
        
        # Check cache
        if domain in self.robots_cache:
            robots_parser = self.robots_cache[domain]
            if robots_parser is None:
                return True
            
            return robots_parser.can_fetch(self.config.user_agent, url)
        
        # If not in cache, assume allowed (will be checked on first request)
        return True
    
    def clear_cache(self):
        """Clear robots.txt cache."""
        self.robots_cache.clear()
        self.disallowed_paths.clear()
    
    def get_stats(self) -> Dict[str, any]:
        """Get rate limiting statistics."""
        return {
            "cached_domains": len(self.robots_cache),
            "domain_delays": dict(self.delays),
            "last_request_times": {
                domain: datetime.fromtimestamp(timestamp).isoformat()
                for domain, timestamp in self.last_request_time.items()
            },
            "off_peak_hours": self.config.off_peak_hours,
            "respect_robots_txt": self.config.respect_robots_txt
        } 