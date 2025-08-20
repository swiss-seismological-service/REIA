import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from reia.services.logger import LoggerService


class TestLoggerService:
    """Test cases for centralized logging service."""

    def setup_method(self):
        """Reset LoggerService state before each test."""
        LoggerService._initialized = False

    def test_ensure_logs_directory_creation(self):
        """Test that logs directory is created when it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = Path.cwd()
            temp_path = Path(temp_dir)

            try:
                # Change to temp directory
                import os
                os.chdir(temp_path)

                # Ensure logs directory doesn't exist
                logs_dir = temp_path / "logs"
                assert not logs_dir.exists()

                # Call the method
                LoggerService._ensure_logs_directory()

                # Verify directory was created
                assert logs_dir.exists()
                assert logs_dir.is_dir()

            finally:
                os.chdir(original_cwd)

    def test_get_logger_returns_logger_instance(self):
        """Test that get_logger returns a proper logger instance."""
        logger = LoggerService.get_logger("test_logger")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_logger"

    @patch('logging.config.fileConfig')
    @patch('os.path.exists')
    def test_setup_logging_with_config_file(self,
                                            mock_exists,
                                            mock_fileconfig):
        """Test setup_logging loads configuration file when it exists."""
        mock_exists.return_value = True

        LoggerService.setup_logging("test_config.ini")

        mock_exists.assert_called_once_with("test_config.ini")
        mock_fileconfig.assert_called_once_with(
            "test_config.ini", disable_existing_loggers=False)

    @patch('logging.config.fileConfig')
    @patch('logging.basicConfig')
    @patch('os.path.exists')
    @patch('importlib.resources.open_text')
    def test_setup_logging_fallback_when_no_config(
            self, mock_open_text, mock_exists, mock_basicconfig,
            mock_fileconfig):
        """Test setup_logging uses basic config when package resource fails."""
        mock_exists.return_value = False
        mock_open_text.side_effect = FileNotFoundError()

        LoggerService.setup_logging("nonexistent_config.ini")

        mock_exists.assert_called_once_with("nonexistent_config.ini")
        mock_open_text.assert_called_once_with("reia.config", "logger.ini")
        mock_basicconfig.assert_called_once()

    @patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'})
    @patch('logging.getLogger')
    def test_setup_logging_respects_log_level_env_var(self, mock_get_logger):
        """Test that LOG_LEVEL environment variable is respected."""
        mock_logger = Mock()
        mock_handler = Mock()
        mock_logger.handlers = [mock_handler]
        mock_get_logger.return_value = mock_logger

        with patch('reia.services.logger.LoggerService._ensure_logs_directory'):
            with patch('importlib.resources.open_text'):
                with patch('logging.config.fileConfig'):
                    LoggerService.setup_logging()

        mock_logger.setLevel.assert_called_once_with(logging.DEBUG)
        mock_handler.setLevel.assert_called_once_with(logging.DEBUG)

    def test_setup_logging_only_initializes_once(self):
        """Test that setup_logging only initializes once."""
        with patch('reia.services.logger.LoggerService._ensure_logs_directory'
                   ) as mock_ensure:
            with patch('importlib.resources.open_text'):
                with patch('logging.config.fileConfig'):
                    LoggerService.setup_logging()
                    LoggerService.setup_logging()  # Second call

        # Should only be called once
        mock_ensure.assert_called_once()


class TestOQCalculationAPILogging:
    """Test cases for OpenQuake API logging integration."""

    def test_log_error_with_traceback_success(self):
        """Test successful traceback logging."""
        from reia.config.settings import get_settings
        from reia.services.oq_api import OQCalculationAPI

        # Create API instance with mocked session
        config = get_settings()
        api = OQCalculationAPI(config)
        api.id = 123
        api.logger = Mock()

        # Mock the get_traceback method
        traceback_lines = ["Error line 1", "Error line 2", "Error line 3"]
        api.get_traceback = Mock(return_value=traceback_lines)

        # Call the method
        api.log_error_with_traceback("Test calculation failed")

        # Verify logging was called with correct format
        expected_message = (
            "Test calculation failed (calc_id: 123)\n"
            "OpenQuake Traceback:\n"
            "Error line 1\nError line 2\nError line 3"
        )
        api.logger.error.assert_called_once_with(expected_message)

    def test_log_error_with_traceback_no_traceback(self):
        """Test logging when no traceback is available."""
        from reia.config.settings import get_settings
        from reia.services.oq_api import OQCalculationAPI

        config = get_settings()
        api = OQCalculationAPI(config)
        api.id = 123
        api.logger = Mock()

        # Mock empty traceback
        api.get_traceback = Mock(return_value=[])

        # Call the method
        api.log_error_with_traceback("Test calculation failed")

        # Verify appropriate message was logged
        expected_message = ("Test calculation failed (calc_id: 123) - "
                            "No traceback available")
        api.logger.error.assert_called_once_with(expected_message)
