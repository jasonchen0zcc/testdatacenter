"""Logging configuration with file rotation and daily retention."""
import logging
import os
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog


def setup_logging(
    log_dir: str = "logs",
    max_bytes: int = 50 * 1024 * 1024,  # 50MB
    backup_count: int = 0,  # 不保留备份，只保留当前文件
    log_level: str = "INFO"
) -> None:
    """
    Setup structured logging with file rotation.

    Features:
    - 50MB file size limit
    - Daily log file (logs/tdc_YYYY-MM-DD.log)
    - Auto cleanup old log files (keep only today)
    """
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Generate log filename with date
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_path / f"tdc_{today}.log"

    # Clean up old log files (keep only today)
    _cleanup_old_logs(log_path, today)

    # Setup structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Get log level
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear existing handlers
    root_logger.handlers = []

    # File handler with rotation (50MB)
    file_handler = RotatingFileHandler(
        filename=str(log_file),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)


def _cleanup_old_logs(log_path: Path, keep_date: str):
    """Remove log files from previous days."""
    for log_file in log_path.glob("tdc_*.log*"):
        # Extract date from filename (tdc_YYYY-MM-DD.log or tdc_YYYY-MM-DD.log.1)
        parts = log_file.stem.split("_")
        if len(parts) >= 2:
            file_date = parts[1]
            if file_date != keep_date:
                try:
                    log_file.unlink()
                    print(f"Cleaned up old log file: {log_file}")
                except OSError as e:
                    print(f"Failed to remove old log file {log_file}: {e}")


def get_logger(name: str = None):
    """Get structured logger instance."""
    return structlog.get_logger(name)
