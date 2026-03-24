"""
Oracle2PostgreSQL - Structured Logger
Provides dual-output logging (console + rotating file) with structured formatting.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional


# Default configuration
_DEFAULT_LOG_DIR = "logs"
_DEFAULT_LOG_FILE = "migration.log"
_DEFAULT_CONSOLE_LEVEL = "INFO"
_DEFAULT_FILE_LEVEL = "DEBUG"
_DEFAULT_MAX_BYTES = 5 * 1024 * 1024  # 5MB
_DEFAULT_BACKUP_COUNT = 3

# Shared format strings
_CONSOLE_FORMAT = "[%(asctime)s][%(levelname)-7s] %(message)s"
_FILE_FORMAT = "[%(asctime)s][%(levelname)-7s][%(name)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Module-level logger cache
_loggers: dict = {}


def setup_logger(
    name: str = "oracle2pg",
    log_file: Optional[str] = None,
    console_level: str = _DEFAULT_CONSOLE_LEVEL,
    file_level: str = _DEFAULT_FILE_LEVEL,
    max_bytes: int = _DEFAULT_MAX_BYTES,
    backup_count: int = _DEFAULT_BACKUP_COUNT,
) -> logging.Logger:
    """
    Create or retrieve a configured logger with console and file handlers.

    Args:
        name:          Logger name (used for hierarchy).
        log_file:      Path to the log file. None = logs/migration.log.
        console_level: Minimum level for console output.
        file_level:    Minimum level for file output.
        max_bytes:     Max size of a single log file before rotation.
        backup_count:  Number of rotated backup files to keep.

    Returns:
        Configured logging.Logger instance.
    """
    # Return cached logger if already set up
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capture everything; handlers filter

    # Prevent duplicate handlers on re-import
    if logger.handlers:
        return logger

    # --- Console handler ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper(), logging.INFO))
    console_handler.setFormatter(logging.Formatter(_CONSOLE_FORMAT, datefmt=_DATE_FORMAT))
    logger.addHandler(console_handler)

    # --- File handler ---
    if log_file is None:
        log_file = os.path.join(_DEFAULT_LOG_DIR, _DEFAULT_LOG_FILE)

    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(getattr(logging, file_level.upper(), logging.DEBUG))
    file_handler.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_DATE_FORMAT))
    logger.addHandler(file_handler)

    _loggers[name] = logger
    return logger


def get_logger(name: str = "oracle2pg") -> logging.Logger:
    """
    Retrieve an existing logger by name, or create a default one.
    """
    if name in _loggers:
        return _loggers[name]
    return setup_logger(name)


class MigrationLogger:
    """
    High-level logging facade for migration operations.
    Provides convenience methods with structured context (file, line, rule).
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._logger = logger or get_logger()

    # ------------------------------------------------------------------
    # Lifecycle events
    # ------------------------------------------------------------------
    def start_migration(self, file_count: int):
        self._logger.info("=" * 60)
        self._logger.info("Migration started  |  files=%d", file_count)
        self._logger.info("=" * 60)

    def finish_migration(self, total: int, auto: int, review: int, manual: int):
        self._logger.info("-" * 60)
        self._logger.info(
            "Migration complete |  files=%d  auto=%d  review=%d  manual=%d",
            total, auto, review, manual,
        )
        self._logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Per-file events
    # ------------------------------------------------------------------
    def start_file(self, filename: str):
        self._logger.info("[PARSE ] %s", filename)

    def transform_file(self, filename: str):
        self._logger.info("[TRANS ] %s", filename)

    def generate_file(self, filename: str, output_path: str):
        self._logger.info("[GEN   ] %s → %s", filename, output_path)

    def file_result(self, filename: str, auto: int, review: int, manual: int):
        self._logger.info(
            "[RESULT] %s  |  auto=%d  review=%d  manual=%d",
            filename, auto, review, manual,
        )

    # ------------------------------------------------------------------
    # Rule-level events
    # ------------------------------------------------------------------
    def rule_applied(self, rule_id: str, category: str, severity: str,
                     description: str, filename: str = "", line: int = 0):
        self._logger.debug(
            "[RULE  ] %s (%s/%s) at %s:%d  →  %s",
            rule_id, category, severity, filename, line, description,
        )

    def rule_skipped(self, rule_id: str, reason: str):
        self._logger.debug("[SKIP  ] %s  reason=%s", rule_id, reason)

    # ------------------------------------------------------------------
    # Warnings and errors
    # ------------------------------------------------------------------
    def parse_warning(self, filename: str, line: int, message: str):
        self._logger.warning("[WARN  ] %s:%d  %s", filename, line, message)

    def parse_error(self, filename: str, line: int, message: str):
        self._logger.error("[ERROR ] %s:%d  %s", filename, line, message)

    def skipped_syntax(self, filename: str, line: int, syntax: str):
        self._logger.warning(
            "[SKIP  ] %s:%d  Unsupported syntax: %.80s",
            filename, line, syntax,
        )

    def error(self, message: str, exc_info: bool = False):
        self._logger.error(message, exc_info=exc_info)

    def info(self, message: str):
        self._logger.info(message)

    def debug(self, message: str):
        self._logger.debug(message)
