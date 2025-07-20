"""
Centralized logging configuration and utilities.

Provides structured logging with JSON format support and configurable output destinations.
Includes request ID tracking and performance monitoring capabilities.
"""

import json
import logging
import logging.config
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import structlog
from structlog.stdlib import LoggerFactory

from ..config import get_config


def setup_logging(
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_format: str = "json"
) -> structlog.stdlib.BoundLogger:
    """
    Set up structured logging for the application.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file. If None, logs to console only.
        log_format: Log format ('json' or 'text')
    """
    config = get_config()
    
    # Use provided level or get from config
    log_level = level or config.logging.level
    
    # Configure structlog
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(colors=True)
        )
    
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper()),
    )
    
    # Add file handler if log file is specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
        )
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Get the root logger and add the file handler
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)

    return structlog.get_logger("root")


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Structured logger instance
    """
    return structlog.get_logger(name)


def log_function_call(func_name: str, **kwargs: Any) -> None:
    """
    Log a function call with its parameters.
    
    Args:
        func_name: Name of the function being called
        **kwargs: Function parameters to log
    """
    logger = get_logger(__name__)
    logger.info(
        "Function called",
        function=func_name,
        parameters=kwargs
    )


def log_scraping_activity(
    portal: str,
    url: str,
    status_code: int,
    response_time: float,
    success: bool,
    error_message: Optional[str] = None
) -> None:
    """
    Log scraping activity for audit and monitoring.
    
    Args:
        portal: Name of the portal being scraped
        url: URL that was scraped
        status_code: HTTP status code
        response_time: Response time in seconds
        success: Whether the scraping was successful
        error_message: Error message if scraping failed
    """
    logger = get_logger(__name__)
    
    log_data = {
        "portal": portal,
        "url": url,
        "status_code": status_code,
        "response_time": response_time,
        "success": success,
    }
    
    if error_message:
        log_data["error_message"] = error_message
    
    if success:
        logger.info("Scraping successful", **log_data)
    else:
        logger.error("Scraping failed", **log_data)


def log_ai_analysis(
    opportunity_id: str,
    model: str,
    analysis_time: float,
    success: bool,
    advantage_score: Optional[float] = None,
    error_message: Optional[str] = None
) -> None:
    """
    Log AI analysis activity.
    
    Args:
        opportunity_id: ID of the opportunity being analyzed
        model: AI model used for analysis
        analysis_time: Time taken for analysis in seconds
        success: Whether the analysis was successful
        advantage_score: Calculated advantage score
        error_message: Error message if analysis failed
    """
    logger = get_logger(__name__)
    
    log_data = {
        "opportunity_id": opportunity_id,
        "model": model,
        "analysis_time": analysis_time,
        "success": success,
    }
    
    if advantage_score is not None:
        log_data["advantage_score"] = advantage_score
    
    if error_message:
        log_data["error_message"] = error_message
    
    if success:
        logger.info("AI analysis completed", **log_data)
    else:
        logger.error("AI analysis failed", **log_data)


def log_database_operation(
    operation: str,
    table: str,
    record_id: Optional[str] = None,
    success: bool = True,
    error_message: Optional[str] = None
) -> None:
    """
    Log database operations.
    
    Args:
        operation: Type of operation (INSERT, UPDATE, DELETE, SELECT)
        table: Database table name
        record_id: ID of the record being operated on
        success: Whether the operation was successful
        error_message: Error message if operation failed
    """
    logger = get_logger(__name__)
    
    log_data = {
        "operation": operation,
        "table": table,
        "success": success,
    }
    
    if record_id:
        log_data["record_id"] = record_id
    
    if error_message:
        log_data["error_message"] = error_message
    
    if success:
        logger.debug("Database operation completed", **log_data)
    else:
        logger.error("Database operation failed", **log_data) 