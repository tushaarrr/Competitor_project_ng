"""Logging utilities for the competitor intelligence system."""
import logging
import sys
import io
from pathlib import Path
from app.config.constants import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger(name: str, log_file: str = None, level: int = logging.INFO) -> logging.Logger:
    """Set up a logger with file and console handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Console handler with UTF-8 encoding for Windows compatibility
    # Don't wrap stdout/stderr here - let the calling script handle it
    # This avoids "I/O operation on closed file" errors

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    # Set encoding error handling to 'replace' to avoid crashes
    if hasattr(console_handler.stream, 'reconfigure'):
        try:
            console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
        except:
            pass
    logger.addHandler(console_handler)

    # File handler (always UTF-8)
    if log_file:
        file_handler = logging.FileHandler(LOG_DIR / log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger

