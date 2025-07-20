"""
ScrapingLog model for audit trail and compliance.

Tracks all scraping activities for compliance monitoring, performance
analysis, and debugging purposes. Ensures ethical scraping practices.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, CheckConstraint, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ScrapingLog(Base):
    """Model for scraping activity audit trail."""
    
    __tablename__ = "scraping_log"
    
    # Portal and URL information
    portal_name: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        comment="Source portal being scraped"
    )
    
    url_scraped: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="URL that was accessed"
    )
    
    # Response information
    status_code: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="HTTP response status code"
    )
    
    response_time_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Response time in milliseconds"
    )
    
    success: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        index=True,
        comment="Whether scraping was successful"
    )
    
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Error details if scraping failed"
    )
    
    # Compliance information
    user_agent: Mapped[Optional[str]] = mapped_column(
        String(200),
        nullable=True,
        comment="User agent string used"
    )
    
    robots_txt_respected: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default="true",
        comment="Whether robots.txt was followed"
    )
    
    rate_limit_delay: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default="0",
        comment="Delay applied in seconds"
    )
    
    # Timestamp
    scraped_at: Mapped[datetime] = mapped_column(
        server_default="now()",
        nullable=False,
        index=True,
        comment="When scraping occurred"
    )
    
    # Constraints
    __table_args__ = (
        CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599",
            name="scraping_log_valid_status"
        ),
        CheckConstraint(
            "response_time_ms IS NULL OR response_time_ms >= 0",
            name="scraping_log_positive_response_time"
        ),
        CheckConstraint(
            "rate_limit_delay >= 0",
            name="scraping_log_positive_delay"
        ),
    )
    
    def __init__(self, **kwargs):
        """Initialize scraping log with validation."""
        super().__init__(**kwargs)
        self._validate_status_code()
        self._validate_response_time()
        self._validate_rate_limit_delay()
    
    def _validate_status_code(self) -> None:
        """Validate HTTP status code."""
        if self.status_code is not None:
            if not (100 <= self.status_code <= 599):
                raise ValueError("Status code must be between 100 and 599")
    
    def _validate_response_time(self) -> None:
        """Validate response time."""
        if self.response_time_ms is not None and self.response_time_ms < 0:
            raise ValueError("Response time must be non-negative")
    
    def _validate_rate_limit_delay(self) -> None:
        """Validate rate limit delay."""
        if self.rate_limit_delay is not None and self.rate_limit_delay < 0:
            raise ValueError("Rate limit delay must be non-negative")
    
    @property
    def is_successful(self) -> bool:
        """Check if scraping was successful."""
        return self.success
    
    @property
    def is_failed(self) -> bool:
        """Check if scraping failed."""
        return not self.success
    
    @property
    def has_error_message(self) -> bool:
        """Check if there's an error message."""
        return bool(self.error_message and self.error_message.strip())
    
    @property
    def status_category(self) -> str:
        """Get HTTP status category."""
        if not self.status_code:
            return "Unknown"
        
        if 100 <= self.status_code < 200:
            return "Informational"
        elif 200 <= self.status_code < 300:
            return "Success"
        elif 300 <= self.status_code < 400:
            return "Redirection"
        elif 400 <= self.status_code < 500:
            return "Client Error"
        elif 500 <= self.status_code < 600:
            return "Server Error"
        else:
            return "Unknown"
    
    @property
    def is_client_error(self) -> bool:
        """Check if status code indicates client error."""
        return 400 <= self.status_code < 500 if self.status_code else False
    
    @property
    def is_server_error(self) -> bool:
        """Check if status code indicates server error."""
        return 500 <= self.status_code < 600 if self.status_code else False
    
    @property
    def is_redirect(self) -> bool:
        """Check if status code indicates redirect."""
        return 300 <= self.status_code < 400 if self.status_code else False
    
    @property
    def response_time_seconds(self) -> Optional[float]:
        """Get response time in seconds."""
        if self.response_time_ms is None:
            return None
        return self.response_time_ms / 1000.0
    
    @property
    def is_slow_response(self) -> bool:
        """Check if response was slow (>5 seconds)."""
        if self.response_time_seconds is None:
            return False
        return self.response_time_seconds > 5.0
    
    @property
    def is_very_slow_response(self) -> bool:
        """Check if response was very slow (>30 seconds)."""
        if self.response_time_seconds is None:
            return False
        return self.response_time_seconds > 30.0
    
    @property
    def domain(self) -> str:
        """Extract domain from URL."""
        from urllib.parse import urlparse
        
        try:
            parsed = urlparse(self.url_scraped)
            return parsed.netloc
        except Exception:
            return "unknown"
    
    @property
    def path(self) -> str:
        """Extract path from URL."""
        from urllib.parse import urlparse
        parsed = urlparse(self.url_scraped)
        # If the URL is not valid (no scheme and netloc), treat as invalid
        if not parsed.scheme or not parsed.netloc:
            return "/"
        return parsed.path if parsed.path else "/"
    
    @property
    def is_compliant(self) -> bool:
        """Check if scraping was compliant with robots.txt and rate limiting."""
        return self.robots_txt_respected and self.rate_limit_delay >= 0
    
    def get_error_preview(self, max_length: int = 200) -> Optional[str]:
        """Get a preview of the error message."""
        if not self.error_message:
            return None
        
        error = self.error_message.strip()
        if len(error) <= max_length:
            return error
        
        return error[:max_length] + "..."
    
    def to_dict(self) -> dict:
        """Convert to dictionary with additional computed fields."""
        base_dict = super().to_dict()
        base_dict.update({
            'status_category': self.status_category,
            'response_time_seconds': self.response_time_seconds,
            'is_slow_response': self.is_slow_response,
            'is_very_slow_response': self.is_very_slow_response,
            'domain': self.domain,
            'path': self.path,
            'is_compliant': self.is_compliant,
            'is_client_error': self.is_client_error,
            'is_server_error': self.is_server_error,
            'is_redirect': self.is_redirect
        })
        return base_dict
    
    @classmethod
    def create_success_log(cls, portal_name: str, url: str, status_code: int,
                          response_time_ms: int, user_agent: Optional[str] = None,
                          robots_respected: bool = True, rate_limit_delay: int = 0) -> "ScrapingLog":
        """Create a successful scraping log entry."""
        return cls(
            portal_name=portal_name,
            url_scraped=url,
            status_code=status_code,
            response_time_ms=response_time_ms,
            success=True,
            user_agent=user_agent,
            robots_txt_respected=robots_respected,
            rate_limit_delay=rate_limit_delay
        )
    
    @classmethod
    def create_failure_log(cls, portal_name: str, url: str, error_message: str,
                          status_code: Optional[int] = None, user_agent: Optional[str] = None,
                          robots_respected: bool = True, rate_limit_delay: int = 0) -> "ScrapingLog":
        """Create a failed scraping log entry."""
        return cls(
            portal_name=portal_name,
            url_scraped=url,
            status_code=status_code,
            success=False,
            error_message=error_message,
            user_agent=user_agent,
            robots_txt_respected=robots_respected,
            rate_limit_delay=rate_limit_delay
        )
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<ScrapingLog(id={self.id}, "
            f"portal='{self.portal_name}', "
            f"url='{self.url_scraped[:50]}...', "
            f"success={self.success}, "
            f"status_code={self.status_code})>"
        ) 