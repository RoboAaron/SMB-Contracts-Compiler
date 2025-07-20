"""
HTTP client management for the scraping infrastructure.

Provides support for multiple HTTP clients (requests, aiohttp, Playwright)
with session management, connection pooling, and retry logic.
"""

import asyncio
import time
from typing import Dict, Optional, Any, Union
from urllib.parse import urlparse

import aiohttp
import requests
from playwright.async_api import async_playwright, Browser, Page

from ..config import ScrapingConfig
from .exceptions import SessionError, ScrapingError


class HTTPClientManager:
    """Manages HTTP clients and sessions for different scraping needs."""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.sessions: Dict[str, Any] = {}
        self.browser: Optional[Browser] = None
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "OpportunityEngine/1.0 (+http://your-company-website.com/bot-info)"
        ]
        self.current_user_agent_index = 0
    
    async def initialize(self):
        """Initialize HTTP clients and sessions."""
        try:
            # Initialize aiohttp session
            connector = aiohttp.TCPConnector(
                limit=self.config.max_connections,
                limit_per_host=self.config.max_connections_per_host,
                ttl_dns_cache=300,
                use_dns_cache=True
            )
            
            timeout = aiohttp.ClientTimeout(
                total=self.config.timeout,
                connect=self.config.timeout // 2
            )
            
            self.sessions['aiohttp'] = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': self.get_user_agent()}
            )
            
            # Initialize requests session
            self.sessions['requests'] = requests.Session()
            self.sessions['requests'].headers.update({
                'User-Agent': self.get_user_agent()
            })
            
            # Initialize Playwright browser
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            
        except Exception as e:
            raise SessionError(f"Failed to initialize HTTP clients: {e}")
    
    def get_user_agent(self) -> str:
        """Get the next user agent in rotation."""
        user_agent = self.user_agents[self.current_user_agent_index]
        self.current_user_agent_index = (self.current_user_agent_index + 1) % len(self.user_agents)
        return user_agent
    
    async def get_session(self, client_type: str = 'aiohttp'):
        """Get or create HTTP session."""
        if client_type not in self.sessions:
            raise SessionError(f"Unsupported client type: {client_type}", client_type)
        return self.sessions[client_type]
    
    async def make_request(
        self, 
        url: str, 
        method: str = 'GET',
        client_type: str = 'aiohttp',
        **kwargs
    ) -> Union[aiohttp.ClientResponse, requests.Response]:
        """Make HTTP request with retry logic."""
        max_retries = self.config.max_retries
        retry_delay = self.config.retry_delay
        
        for attempt in range(max_retries + 1):
            try:
                if client_type == 'aiohttp':
                    return await self._make_aiohttp_request(url, method, **kwargs)
                elif client_type == 'requests':
                    return self._make_requests_request(url, method, **kwargs)
                elif client_type == 'playwright':
                    return await self._make_playwright_request(url, **kwargs)
                else:
                    raise SessionError(f"Unsupported client type: {client_type}", client_type)
                    
            except Exception as e:
                if attempt == max_retries:
                    raise ScrapingError(f"Request failed after {max_retries} retries: {e}", url)
                
                await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
    
    async def _make_aiohttp_request(self, url: str, method: str, **kwargs) -> aiohttp.ClientResponse:
        """Make request using aiohttp."""
        session = await self.get_session('aiohttp')
        
        # Update headers with current user agent
        headers = kwargs.get('headers', {})
        headers['User-Agent'] = self.get_user_agent()
        kwargs['headers'] = headers
        
        async with session.request(method, url, **kwargs) as response:
            # Read the response content to ensure it's available
            await response.read()
            return response
    
    def _make_requests_request(self, url: str, method: str, **kwargs) -> requests.Response:
        """Make request using requests."""
        session = self.sessions['requests']
        
        # Update headers with current user agent
        headers = kwargs.get('headers', {})
        headers['User-Agent'] = self.get_user_agent()
        kwargs['headers'] = headers
        
        response = session.request(method, url, **kwargs)
        return response
    
    async def _make_playwright_request(self, url: str, **kwargs) -> str:
        """Make request using Playwright (returns HTML content)."""
        if not self.browser:
            raise SessionError("Playwright browser not initialized", "playwright")
        
        page = await self.browser.new_page()
        try:
            # Set user agent
            await page.set_extra_http_headers({
                'User-Agent': self.get_user_agent()
            })
            
            # Navigate to URL
            await page.goto(url, wait_until='networkidle', timeout=self.config.timeout * 1000)
            
            # Get page content
            content = await page.content()
            return content
            
        finally:
            await page.close()
    
    async def get_page_with_playwright(self, url: str) -> Page:
        """Get a Playwright page for complex interactions."""
        if not self.browser:
            raise SessionError("Playwright browser not initialized", "playwright")
        
        page = await self.browser.new_page()
        await page.set_extra_http_headers({
            'User-Agent': self.get_user_agent()
        })
        return page
    
    def get_domain(self, url: str) -> str:
        """Extract domain from URL for rate limiting."""
        return urlparse(url).netloc
    
    async def cleanup(self):
        """Clean up HTTP clients and sessions."""
        try:
            # Close aiohttp session
            if 'aiohttp' in self.sessions:
                await self.sessions['aiohttp'].close()
            
            # Close requests session
            if 'requests' in self.sessions:
                self.sessions['requests'].close()
            
            # Close Playwright browser
            if self.browser:
                await self.browser.close()
                
        except Exception as e:
            # Log cleanup errors but don't raise
            print(f"Warning: Error during HTTP client cleanup: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        asyncio.create_task(self.cleanup()) 