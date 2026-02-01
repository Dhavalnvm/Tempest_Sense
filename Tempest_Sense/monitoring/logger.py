"""
Centralized Logging Configuration
Provides structured logging for all components
"""
import os
import sys
import logging
import json
from datetime import datetime
from typing import Optional


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'  # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors"""
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']

        # Add color to level name
        record.levelname = f"{color}{record.levelname}{reset}"

        return super().format(record)


def setup_logging(
        level: str = 'INFO',
        format_type: str = 'standard',
        log_file: Optional[str] = None,
        service_name: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the application

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_type: 'json', 'colored', or 'standard'
        log_file: Optional file path for file logging
        service_name: Optional service name to include in logs

    Returns:
        Configured logger instance
    """
    # Get log level from environment or parameter
    log_level = os.getenv('LOG_LEVEL', level).upper()
    format_type = os.getenv('LOG_FORMAT', format_type).lower()

    # Create logger
    logger = logging.getLogger(service_name or 'cyclone')
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))

    # Choose formatter based on type
    if format_type == 'json':
        formatter = JSONFormatter()
    elif format_type == 'colored':
        formatter = ColoredFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    else:  # standard
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level, logging.INFO))

        # Always use JSON format for file logs
        file_formatter = JSONFormatter()
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        logger.info(f"File logging enabled: {log_file}")

    # Prevent propagation to root logger
    logger.propagate = False

    logger.info(f"Logging configured: level={log_level}, format={format_type}")

    return logger


def get_logger(name: str = 'cyclone') -> logging.Logger:
    """
    Get a logger instance with the configured settings

    Args:
        name: Logger name (usually module name)

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)

    # If logger has no handlers, set it up
    if not logger.handlers:
        setup_logging(service_name=name)

    return logger


class LogContext:
    """Context manager for adding extra fields to log records"""

    def __init__(self, logger: logging.Logger, **kwargs):
        self.logger = logger
        self.extra_fields = kwargs
        self.old_factory = None

    def __enter__(self):
        self.old_factory = logging.getLogRecordFactory()

        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            record.extra_fields = self.extra_fields
            return record

        logging.setLogRecordFactory(record_factory)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.setLogRecordFactory(self.old_factory)


# Convenience functions for different log levels
def debug(message: str, **kwargs):
    """Log debug message"""
    logger = get_logger()
    logger.debug(message, extra={'extra_fields': kwargs})


def info(message: str, **kwargs):
    """Log info message"""
    logger = get_logger()
    logger.info(message, extra={'extra_fields': kwargs})


def warning(message: str, **kwargs):
    """Log warning message"""
    logger = get_logger()
    logger.warning(message, extra={'extra_fields': kwargs})


def error(message: str, **kwargs):
    """Log error message"""
    logger = get_logger()
    logger.error(message, extra={'extra_fields': kwargs})


def critical(message: str, **kwargs):
    """Log critical message"""
    logger = get_logger()
    logger.critical(message, extra={'extra_fields': kwargs})


# Example usage
if __name__ == "__main__":
    # Test different log formats

    print("=" * 60)
    print("Testing STANDARD format:")
    print("=" * 60)
    logger = setup_logging(level='DEBUG', format_type='standard', service_name='test')
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    print("\n" + "=" * 60)
    print("Testing COLORED format:")
    print("=" * 60)
    logger = setup_logging(level='DEBUG', format_type='colored', service_name='test')
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    print("\n" + "=" * 60)
    print("Testing JSON format:")
    print("=" * 60)
    logger = setup_logging(level='DEBUG', format_type='json', service_name='test')
    logger.debug("This is a debug message")
    logger.info("This is an info message", extra={'extra_fields': {'user_id': 123, 'action': 'fetch'}})
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    # Test exception logging
    try:
        raise ValueError("Test exception")
    except Exception as e:
        logger.exception("Exception occurred")

    print("\n" + "=" * 60)
    print("Testing LogContext:")
    print("=" * 60)
    logger = setup_logging(format_type='json', service_name='test')
    with LogContext(logger, request_id='abc123', user='admin'):
        logger.info("Message with context")