"""
Custom exceptions for the scraping infrastructure.
"""


class ScrapingError(Exception):
    """Base exception for all scraping-related errors."""
    
    def __init__(self, message: str, url: str = None, status_code: int = None):
        self.message = message
        self.url = url
        self.status_code = status_code
        super().__init__(self.message)


class RateLimitError(ScrapingError):
    """Raised when rate limiting is enforced."""
    
    def __init__(self, message: str, delay: float = None, url: str = None):
        self.delay = delay
        super().__init__(message, url)


class RobotsTxtError(ScrapingError):
    """Raised when robots.txt parsing fails or access is disallowed."""
    
    def __init__(self, message: str, robots_url: str = None):
        self.robots_url = robots_url
        super().__init__(message, robots_url)


class ContentExtractionError(ScrapingError):
    """Raised when content extraction fails."""
    
    def __init__(self, message: str, content_type: str = None, url: str = None):
        self.content_type = content_type
        super().__init__(message, url)


class ValidationError(ScrapingError):
    """Raised when scraped data fails validation."""
    
    def __init__(self, message: str, field: str = None, value: str = None):
        self.field = field
        self.value = value
        super().__init__(message)


class SessionError(ScrapingError):
    """Raised when session management fails."""
    
    def __init__(self, message: str, session_type: str = None):
        self.session_type = session_type
        super().__init__(message)


class ParsingError(ScrapingError):
    """Raised when HTML parsing or data extraction fails."""
    
    def __init__(self, message: str, content: str = None, selector: str = None):
        self.content = content
        self.selector = selector
        super().__init__(message)


class DocumentDownloadError(ScrapingError):
    """Raised when document download fails."""
    
    def __init__(self, message: str, document_url: str = None, file_path: str = None):
        self.document_url = document_url
        self.file_path = file_path
        super().__init__(message, document_url) 