"""
Configuration management for the Texas WBE Opportunity Discovery Engine.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class DatabaseConfig(BaseModel):
    """Database configuration settings."""
    host: str = "localhost"
    port: int = 5432
    name: str = "wbe_opportunities"
    user: str = "wbe_user"
    password: str = Field(default_factory=lambda: os.getenv("DATABASE_PASSWORD", ""))
    pool_size: int = 10
    max_overflow: int = 20
    echo: bool = False

    @field_validator("password", mode="before")
    @classmethod
    def get_password_from_env(cls, v):
        if v is None:
            return os.getenv("DATABASE_PASSWORD", "")
        return v


class AIConfig(BaseModel):
    """AI/ML configuration settings."""
    provider: str = "google"
    model: str = "gemini-pro"
    api_key: str = Field(default_factory=lambda: os.getenv("GOOGLE_AI_API_KEY", ""))
    max_tokens: int = 4096
    temperature: float = 0.1
    timeout: int = 30

    @field_validator("api_key", mode="before")
    @classmethod
    def get_api_key_from_env(cls, v):
        env_key = os.getenv("GOOGLE_AI_API_KEY")
        if env_key:
            return env_key
        return v or ""


class ScrapingConfig(BaseModel):
    """Web scraping configuration settings."""
    user_agent: str = "OpportunityEngine/1.0 (+http://your-company-website.com/bot-info)"
    request_delay: float = 3.0
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    max_connections: int = 100
    max_connections_per_host: int = 10
    respect_robots_txt: bool = True
    off_peak_hours: Dict[str, str] = Field(default_factory=lambda: {"start": "23:00", "end": "06:00"})


class PortalConfig(BaseModel):
    """Individual portal configuration."""
    name: str
    base_url: str
    enabled: bool = True
    priority: int = 1


class NIGPCodeConfig(BaseModel):
    """NIGP code configuration for business lines."""
    class_code: str
    item_code: str
    description: str
    relevance_tier: int = 1


class ScoringConfig(BaseModel):
    """Advantage scoring configuration."""
    high_weight_factors: Dict[str, float] = Field(default_factory=dict)
    medium_weight_factors: Dict[str, float] = Field(default_factory=dict)
    low_weight_factors: Dict[str, float] = Field(default_factory=dict)


class EmailConfig(BaseModel):
    """Email configuration for reporting."""
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    username: str = Field(default_factory=lambda: os.getenv("SMTP_USERNAME", ""))
    password: str = Field(default_factory=lambda: os.getenv("SMTP_PASSWORD", ""))
    from_address: str = "noreply@wbe-opportunity-engine.com"
    to_addresses: List[str] = Field(default_factory=lambda: [addr.strip() for addr in os.getenv("EMAIL_TO_ADDRESSES", "").split(",") if addr.strip()])

    @field_validator("username", "password", mode="before")
    @classmethod
    def get_credentials_from_env(cls, v, info):
        if v is None:
            env_var = f"SMTP_{info.field_name.upper()}"
            return os.getenv(env_var, "")
        return v

    @field_validator("to_addresses", mode="before")
    @classmethod
    def get_to_addresses_from_env(cls, v):
        if v is None:
            env_addresses = os.getenv("EMAIL_TO_ADDRESSES", "")
            if env_addresses:
                return [addr.strip() for addr in env_addresses.split(",") if addr.strip()]
            return []
        return v


class ReportingConfig(BaseModel):
    """Reporting configuration."""
    schedule: str = "daily"  # daily, weekly
    delivery_methods: List[str] = Field(default_factory=lambda: ["email"])
    email: EmailConfig = Field(default_factory=EmailConfig)
    report_template: str = "templates/opportunity_report.html"
    max_opportunities_per_report: int = 50


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "json"
    handlers: List[Dict[str, Any]] = Field(default_factory=list)


class MonitoringConfig(BaseModel):
    """Monitoring configuration."""
    enabled: bool = True
    metrics_port: int = 8000
    health_check_endpoint: str = "/health"
    prometheus_metrics: bool = True


class AppConfig(BaseModel):
    """Application configuration."""
    name: str = "Texas WBE Opportunity Discovery Engine"
    version: str = "0.1.0"
    environment: str = Field(default_factory=lambda: os.getenv("APP_ENVIRONMENT", "development"))
    debug: bool = Field(default_factory=lambda: os.getenv("APP_DEBUG", "").lower() in ("true", "1", "yes"))

    @field_validator("environment", mode="before")
    @classmethod
    def get_environment_from_env(cls, v):
        if v is None:
            return os.getenv("APP_ENVIRONMENT", "development")
        return v

    @field_validator("debug", mode="before")
    @classmethod
    def get_debug_from_env(cls, v):
        if v is None:
            env_debug = os.getenv("APP_DEBUG", "")
            if env_debug:
                return env_debug.lower() in ("true", "1", "yes")
            return False
        return v


class ScraperConfig(BaseModel):
    """Individual scraper configuration."""
    name: Optional[str] = None
    enabled: bool = True
    priority: int = 1
    base_url: str
    search_url: Optional[str] = None
    detail_url: Optional[str] = None
    document_base_url: Optional[str] = None
    extraction_mode: str = "standardized"  # standardized, optimized, hybrid
    optimization_level: str = "balanced"  # balanced, speed, quality
    performance: Dict[str, Any] = Field(default_factory=dict)
    field_mappings: Dict[str, Any] = Field(default_factory=dict)
    selectors: Dict[str, str] = Field(default_factory=dict)
    requires_selenium: bool = False
    rate_limit: int = 2
    selenium: Dict[str, Any] = Field(default_factory=dict)


class Config(BaseModel):
    """Main configuration class."""
    app: AppConfig = Field(default_factory=AppConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    ai: AIConfig = Field(default_factory=AIConfig)
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    scrapers: Dict[str, ScraperConfig] = Field(default_factory=dict)
    portals: Dict[str, PortalConfig] = Field(default_factory=dict)
    nigp_codes: Dict[str, List[NIGPCodeConfig]] = Field(default_factory=dict)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from YAML file and environment variables.
    
    Args:
        config_path: Path to configuration file. If None, uses default location.
        
    Returns:
        Config object with all settings.
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "default.yaml"
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f)
    
    return Config(**config_data)


# Global configuration instance
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        Config object with all settings.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reload_config(config_path: Optional[str] = None) -> Config:
    """
    Reload configuration from file.
    
    Args:
        config_path: Path to configuration file. If None, uses default location.
        
    Returns:
        Updated Config object.
    """
    global _config
    _config = load_config(config_path)
    return _config 