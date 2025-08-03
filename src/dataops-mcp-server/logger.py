#!/usr/bin/env python3
"""
Common Logger Module for DataOps MCP Server

Provides standardized logging configuration across all modules including:
- Structured logging with consistent formatting
- Multiple log levels and handlers
- JSON logging for production environments
- Debug mode support
- Performance tracking
- Error reporting with context
"""

import logging
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import structlog
from functools import wraps


# Default logging configuration
DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Performance tracking logger
PERFORMANCE_LOGGER_NAME = 'dataops.performance'


class PerformanceLogger:
    """Logger for tracking performance metrics and timing."""
    
    def __init__(self, name: str = PERFORMANCE_LOGGER_NAME):
        self.logger = logging.getLogger(name)
    
    def log_timing(self, operation: str, duration_ms: float, context: Dict[str, Any] = None):
        """Log timing information for operations."""
        context = context or {}
        self.logger.info(
            f"Performance: {operation}",
            extra={
                'operation': operation,
                'duration_ms': duration_ms,
                'timestamp': datetime.now().isoformat(),
                **context
            }
        )
    
    def log_query_performance(self, query_type: str, duration_ms: float, 
                            rows_processed: int = 0, cost_usd: float = 0):
        """Log BigQuery performance metrics."""
        self.logger.info(
            f"Query Performance: {query_type}",
            extra={
                'query_type': query_type,
                'duration_ms': duration_ms,
                'rows_processed': rows_processed,
                'cost_usd': cost_usd,
                'timestamp': datetime.now().isoformat()
            }
        )


class StructuredLogger:
    """Structured logger with JSON output for production environments."""
    
    def __init__(self, name: str, enable_json: bool = False):
        self.name = name
        self.enable_json = enable_json
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup structured logging with optional JSON output."""
        logger = logging.getLogger(self.name)
        
        if self.enable_json:
            # Configure structlog for JSON output
            structlog.configure(
                processors=[
                    structlog.stdlib.filter_by_level,
                    structlog.stdlib.add_logger_name,
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.PositionalArgumentsFormatter(),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.UnicodeDecoder(),
                    structlog.processors.JSONRenderer()
                ],
                context_class=dict,
                logger_factory=structlog.stdlib.LoggerFactory(),
                cache_logger_on_first_use=True,
            )
        
        return logger
    
    def info(self, message: str, **context):
        """Log info message with context."""
        if self.enable_json:
            log = structlog.get_logger(self.name)
            log.info(message, **context)
        else:
            self.logger.info(f"{message} - Context: {context}" if context else message)
    
    def error(self, message: str, error: Exception = None, **context):
        """Log error message with context and exception details."""
        error_context = {
            'error_type': type(error).__name__ if error else 'Unknown',
            'error_message': str(error) if error else 'No error details',
            **context
        }
        
        if self.enable_json:
            log = structlog.get_logger(self.name)
            log.error(message, **error_context)
        else:
            self.logger.error(f"{message} - Context: {error_context}")
    
    def debug(self, message: str, **context):
        """Log debug message with context."""
        if self.enable_json:
            log = structlog.get_logger(self.name)
            log.debug(message, **context)
        else:
            self.logger.debug(f"{message} - Context: {context}" if context else message)


def setup_logging(
    level: str = "INFO",
    enable_debug: bool = False,
    enable_json: bool = False,
    log_file: Optional[str] = None,
    module_name: str = "dataops-mcp-server"
) -> logging.Logger:
    """
    Setup standardized logging configuration for the entire application.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_debug: Enable debug logging
        enable_json: Enable JSON structured logging
        log_file: Optional log file path
        module_name: Name of the module for logger identification
    
    Returns:
        Configured logger instance
    """
    # Convert string level to logging level
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Override level if debug is enabled
    if enable_debug:
        log_level = logging.DEBUG
    
    # Create root logger
    logger = logging.getLogger(module_name)
    logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    
    # Choose formatter based on JSON preference
    if enable_json:
        # Simple JSON formatter for console
        formatter = logging.Formatter('{"timestamp": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
    else:
        formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Performance logger setup
    perf_logger = logging.getLogger(PERFORMANCE_LOGGER_NAME)
    perf_logger.setLevel(logging.INFO)
    perf_handler = logging.StreamHandler(sys.stdout)
    perf_formatter = logging.Formatter('PERF - %(asctime)s - %(message)s', DEFAULT_DATE_FORMAT)
    perf_handler.setFormatter(perf_formatter)
    perf_logger.addHandler(perf_handler)
    
    logger.info(f"Logging configured - Level: {level}, Debug: {enable_debug}, JSON: {enable_json}")
    
    return logger


def get_logger(name: str, enable_json: bool = False) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Logger name (typically __name__)
        enable_json: Enable JSON structured logging
    
    Returns:
        Logger instance
    """
    if enable_json:
        return StructuredLogger(name, enable_json=True).logger
    else:
        return logging.getLogger(name)


def log_function_call(logger: logging.Logger = None):
    """
    Decorator to log function calls with parameters and execution time.
    
    Args:
        logger: Optional logger instance, will create one if not provided
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_logger = logger or logging.getLogger(func.__module__)
            start_time = datetime.now()
            
            # Log function entry
            func_logger.debug(f"Entering {func.__name__} with args: {args}, kwargs: {kwargs}")
            
            try:
                result = func(*args, **kwargs)
                
                # Calculate execution time
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                # Log successful completion
                func_logger.debug(f"Completed {func.__name__} in {duration_ms:.2f}ms")
                
                # Log to performance logger
                perf_logger = PerformanceLogger()
                perf_logger.log_timing(
                    operation=f"{func.__module__}.{func.__name__}",
                    duration_ms=duration_ms,
                    context={'success': True}
                )
                
                return result
                
            except Exception as e:
                # Calculate execution time for failed calls
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                # Log error
                func_logger.error(f"Error in {func.__name__}: {e}")
                
                # Log to performance logger
                perf_logger = PerformanceLogger()
                perf_logger.log_timing(
                    operation=f"{func.__module__}.{func.__name__}",
                    duration_ms=duration_ms,
                    context={'success': False, 'error': str(e)}
                )
                
                raise
        
        return wrapper
    return decorator


def log_bigquery_operation(operation_type: str):
    """
    Decorator specifically for BigQuery operations to track performance.
    
    Args:
        operation_type: Type of BigQuery operation (query, health_check, etc.)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            perf_logger = PerformanceLogger()
            start_time = datetime.now()
            
            logger.info(f"Starting BigQuery operation: {operation_type}")
            
            try:
                result = func(*args, **kwargs)
                
                # Calculate timing
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                # Extract metrics from result if it's JSON
                cost_usd = 0
                rows_processed = 0
                try:
                    if isinstance(result, str):
                        result_data = json.loads(result)
                        if result_data.get('success'):
                            data = result_data.get('data', {})
                            cost_usd = data.get('total_cost_usd', 0)
                            rows_processed = data.get('total_queries', 0)
                except:
                    pass  # Don't fail if we can't extract metrics
                
                # Log performance
                perf_logger.log_query_performance(
                    query_type=operation_type,
                    duration_ms=duration_ms,
                    rows_processed=rows_processed,
                    cost_usd=cost_usd
                )
                
                logger.info(f"Completed BigQuery operation: {operation_type} in {duration_ms:.2f}ms")
                return result
                
            except Exception as e:
                end_time = datetime.now()
                duration_ms = (end_time - start_time).total_seconds() * 1000
                
                logger.error(f"BigQuery operation failed: {operation_type} - {e}")
                perf_logger.log_timing(
                    operation=f"bigquery.{operation_type}",
                    duration_ms=duration_ms,
                    context={'success': False, 'error': str(e)}
                )
                raise
        
        return wrapper
    return decorator


# Pre-configured loggers for common use cases
def get_server_logger(debug: bool = False) -> logging.Logger:
    """Get logger for MCP server operations."""
    return setup_logging(
        level="DEBUG" if debug else "INFO",
        enable_debug=debug,
        module_name="dataops.server"
    )


def get_bigquery_logger(debug: bool = False) -> logging.Logger:
    """Get logger for BigQuery operations."""
    return setup_logging(
        level="DEBUG" if debug else "INFO",
        enable_debug=debug,
        module_name="dataops.bigquery"
    )


def get_client_logger(debug: bool = False) -> logging.Logger:
    """Get logger for client operations."""
    return setup_logging(
        level="DEBUG" if debug else "INFO",
        enable_debug=debug,
        module_name="dataops.client"
    )


# Export commonly used functions and classes
__all__ = [
    'setup_logging',
    'get_logger',
    'log_function_call',
    'log_bigquery_operation',
    'PerformanceLogger',
    'StructuredLogger',
    'get_server_logger',
    'get_bigquery_logger',
    'get_client_logger'
]
