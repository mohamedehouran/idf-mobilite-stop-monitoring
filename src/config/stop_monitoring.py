import json
import pandas as pd
import multiprocessing
from pathlib import Path
from functools import cached_property
from typing import Optional, Dict, Tuple, List
from src.config.app import AppConfig
from src.config.config_validator import (
    validate_file_exists,
    validate_required_vars,
    validate_positive_value,
)


class StopMonitoringConfig:
    """
    Configuration handler for the Stop Monitoring service.
    """

    def __init__(self, app_config: AppConfig, selected_towns: str) -> None:
        self.dir_manager = app_config.directory_manager
        self.env_manager = app_config.environment_manager
        self.selected_towns = self._get_selected_towns(selected_towns)

        self.max_workers, self.idf_mobilite_api_key = self._get_env_vars()
        self._validate_config()
        self.processed_file_path = self._get_processed_file_path()

    def _get_selected_towns(self, selected_towns: str) -> Optional[List[str]]:
        """
        Parses the comma-separated selected towns string into a tuple.
        """
        return (
            [town.strip() for town in selected_towns.split(",")]
            if selected_towns
            else None
        )

    def _validate_config(self) -> None:
        """
        Centralized configuration validation.
        """
        var = self.env_manager.variables
        validate_positive_value({var.MAX_WORKERS.value: self.max_workers})
        validate_required_vars(
            {
                var.IDF_MOBILITE_API_KEY.value: self.idf_mobilite_api_key,
                var.SELECTED_TOWNS.value: self.selected_towns,
            }
        )

    def _get_env_vars(self) -> Tuple[str]:
        """
        Retrieves necessary environment variables for the service.
        """
        var = self.env_manager.variables
        max_workers = self.env_manager.get_environment_var(
            var.MAX_WORKERS, max(1, multiprocessing.cpu_count() - 1)
        )
        idf_mobilite_api_key = self.env_manager.get_environment_var(
            var.IDF_MOBILITE_API_KEY
        )
        return (max_workers, idf_mobilite_api_key)

    def _get_processed_file_path(self) -> Path:
        """
        Returns the path where the processed file will be saved.
        """
        return (
            self.dir_manager.get_directory_path(self.dir_manager.directories.DATA)
            / "stop_monitoring.csv"
        )

    def get_request_url(self, stop_point_id: str) -> str:
        """
        Returns a request URL for a specific stop point.
        """
        url_template = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring?MonitoringRef=STIF:StopPoint:Q:"
        return f"{url_template}{stop_point_id}:"

    @cached_property
    def headers(self) -> Dict[str, str]:
        """
        Headers for API requests.
        """
        return {
            "Accept": "application/json",
            "apikey": self.idf_mobilite_api_key,
        }


class StopReferentialConfig:
    """
    Configuration for the Stop Referential service.
    """

    def __init__(self, app_config: AppConfig) -> None:
        self.dir_manager = app_config.directory_manager
        self.referential_file_path = self._get_referential_file_path()
        validate_file_exists(self.referential_file_path)

    def _get_referential_file_path(self) -> Path:
        """
        Returns the path to the stop referential file.
        """
        return (
            self.dir_manager.get_directory_path(self.dir_manager.directories.CONFIG)
            / "stop_referential.json"
        )

    def load_referential(self) -> pd.DataFrame:
        """
        Loads the stop referential from the JSON file and return it as a DataFrame.
        """
        try:
            with open(self.referential_file_path) as file:
                data = json.load(file)
            return pd.DataFrame(data)
        except Exception as e:
            RuntimeError(f"Unexpected error occurred reading referential file : {e}")
