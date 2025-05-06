import logging
from enum import Enum
from typing import List
from pathlib import Path
from src.config.app import AppConfig, app_config


logger = logging.getLogger(__name__)


class LoggerConfig:
    """
    Configures the application's logging system.
    """

    class Key(Enum):
        FILENAME = "app.log"
        FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        DATEFMT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, app_config: AppConfig, level: int = logging.INFO):
        self.dir_manager = app_config.directory_manager
        self.level = level

    def _get_logs_file_path(self) -> Path:
        """
        Returns the log file path.
        """
        return (
            self.dir_manager.get_directory_path(self.dir_manager.directories.LOGS)
            / self.Key.FILENAME.value
        )

    def _get_handlers(self) -> List[logging.Handler]:
        """ "
        Create and return configured log handlers.
        """
        return [
            logging.FileHandler(self._get_logs_file_path()),
            logging.StreamHandler(),
        ]

    def configure_logging(self) -> None:
        """
        Configures the logging system.
        """
        try:
            logging.basicConfig(
                format=LoggerConfig.Key.FORMAT.value,
                datefmt=LoggerConfig.Key.DATEFMT.value,
                level=self.level,
                force=True,
                handlers=self._get_handlers(),
            )
            logger.debug("Logging initialized successfully")
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error occurred while configuring logging : {e}"
            )


logger_config = LoggerConfig(app_config)
logger_config.configure_logging()
