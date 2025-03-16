# custom_logger.py

import logging
import os
from datetime import datetime
from rich.logging import RichHandler

class CustomLogger:
    """
    A custom logger that attaches a Rich console handler and a file handler.
    Usage:
        from custom_logger import CustomLogger
        logger = CustomLogger(__name__).get_logger()
        logger.info("Hello from my module!")
    """
    _console_handler = None
    _file_handler = None

    def __init__(self, name: str, level=logging.INFO):
        """
        Initialize the logger by name and set the logging level.

        Args:
            name (str): The name of the logger.
            level (int): The logging level (default: logging.INFO).
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Initialize class-level handlers only once
        if CustomLogger._console_handler is None or CustomLogger._file_handler is None:
            self._initialize_handlers()

        # Attach the console handler if not already attached
        if not self._has_handler(CustomLogger._console_handler):
            self.logger.addHandler(CustomLogger._console_handler)

        # Attach the file handler if not already attached
        if not self._has_handler(CustomLogger._file_handler):
            self.logger.addHandler(CustomLogger._file_handler)

    @staticmethod
    def _initialize_handlers():
        """
        Create a Rich console handler and a file handler. Store them
        in static class variables so they're shared by all loggers.
        """
        os.makedirs("logs", exist_ok=True)
        log_filename = f"logs/fetcher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # --- RICH CONSOLE HANDLER ---
        # IMPORTANT: Do NOT attach a normal Formatter to RichHandler.
        console_handler = RichHandler(
            markup=True,
            show_time=True,
            show_level=True,
            rich_tracebacks=True,
            # This controls how the time is displayed in the console
            log_time_format="[%H:%M:%S]"
        )

        # --- FILE HANDLER ---
        # For file logs, we use a standard Formatter (plain text).
        file_handler = logging.FileHandler(log_filename, mode="a", encoding="utf-8")
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="[%Y-%m-%d %H:%M:%S]"
        )
        file_handler.setFormatter(file_formatter)

        CustomLogger._console_handler = console_handler
        CustomLogger._file_handler = file_handler

    def _has_handler(self, handler: logging.Handler) -> bool:
        """Check if the logger already has the specified handler."""
        return any(h is handler for h in self.logger.handlers)

    def get_logger(self):
        """Return the configured logger instance."""
        return self.logger