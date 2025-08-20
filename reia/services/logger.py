import logging
import logging.config
import os
from importlib import resources
from pathlib import Path


class LoggerService:
    """Centralized logging service for REIA."""

    _initialized = False

    @staticmethod
    def setup_logging(config_path: str = None) -> None:
        """Setup centralized logging configuration.

        Args:
            config_path: Path to logging configuration file
                        (optional, uses package resource by default)
        """
        # Prevent multiple initializations
        if LoggerService._initialized:
            return

        # Ensure logs directory exists
        LoggerService._ensure_logs_directory()

        # Load logging configuration
        try:
            if config_path and os.path.exists(config_path):
                # Use provided external config path
                logging.config.fileConfig(
                    config_path, disable_existing_loggers=False)
            else:
                # Use package resource
                with resources.open_text("reia.config", "logger.ini"
                                         ) as config_file:
                    logging.config.fileConfig(
                        config_file, disable_existing_loggers=False)
        except (FileNotFoundError, ImportError):
            # Fallback to basic configuration
            LoggerService._setup_basic_logging()

        # Apply environment variable log level if specified
        log_level = os.environ.get('LOG_LEVEL', '').upper()
        if log_level in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            logging.getLogger().setLevel(getattr(logging, log_level))
            # Also update all handlers
            for handler in logging.getLogger().handlers:
                handler.setLevel(getattr(logging, log_level))

        LoggerService._initialized = True

    @staticmethod
    def _ensure_logs_directory() -> None:
        """Create logs directory if it doesn't exist."""
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

    @staticmethod
    def _setup_basic_logging() -> None:
        """Setup basic logging configuration as fallback."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - [%(filename)s.%(funcName)s] '
            '- %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get a logger instance with the specified name.

        Args:
            name: Logger name (typically __name__)

        Returns:
            Configured logger instance
        """
        return logging.getLogger(name)
