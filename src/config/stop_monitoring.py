import multiprocessing
from typing import Dict
from pathlib import Path
from dataclasses import dataclass, field
from src.config.app import AppConfig
from src.config.config_validator import (
    validate_file_exists,
    validate_required_vars,
    validate_positive_value,
)


@dataclass
class StopMonitoringDataProcessorConfig:
    """
    Manage configuration for stop monitoring data processing.
    """

    app_config: AppConfig
    max_workers: int = field(init=False)

    def __post_init__(self):
        self.dir_manager = self.app_config.directory_manager
        self.env_manager = self.app_config.environment_manager
        self.max_workers = int(
            self.env_manager.get_environment_var(
                self.env_manager.variables.MAX_WORKERS,
                max(1, multiprocessing.cpu_count() - 1),
            )
        )

        validate_positive_value(
            {self.env_manager.variables.MAX_WORKERS.value: self.max_workers}
        )


@dataclass
class StopMonitoringConfig:
    """
    Configure Stop Monitoring API entry point.
    """

    data_processor: StopMonitoringDataProcessorConfig
    output_filename: str = "stop_monitoring"
    referential_file_path: Path = field(init=False)
    idf_mobilite_api_key: str = field(init=False)
    headers: Dict[str, str] = field(init=False)

    def __post_init__(self):
        self.dir_manager = self.data_processor.dir_manager
        self.env_manager = self.data_processor.env_manager
        self.referential_file_path = (
            self.dir_manager.get_directory_path(self.dir_manager.directories.CONFIG)
            / "stop_referential.json"
        )
        self.idf_mobilite_api_key = self.env_manager.get_environment_var(
            self.env_manager.variables.IDF_MOBILITE_API_KEY
        )
        self.headers = {
            "Accept": "application/json",
            "apikey": self.idf_mobilite_api_key,
        }

        self._validate_config()

    def _validate_config(self):
        """
        Centralized configuration validation.
        """
        validate_file_exists(self.referential_file_path)
        validate_required_vars(
            {
                self.env_manager.variables.IDF_MOBILITE_API_KEY.value: self.idf_mobilite_api_key
            }
        )

    def get_request_url(self, stop_point_id: str) -> str:
        """
        Return a request URL for a specific stop point.
        """
        url_template = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring?MonitoringRef=STIF:StopPoint:Q:"
        return f"{url_template}{stop_point_id}:"
