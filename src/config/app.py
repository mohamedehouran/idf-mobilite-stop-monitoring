import os
from enum import Enum
from typing import Dict
from pathlib import Path
from dataclasses import dataclass
from functools import cached_property


class EnvironmentVars(Enum):
    """
    Environment variables used by the application.
    """

    IDF_MOBILITE_API_KEY = "IDF_MOBILITE_API_KEY"
    SELECTED_TOWNS = "SELECTED_TOWNS"
    MAX_WORKERS = "MAX_WORKERS"


@dataclass(frozen=True)
class EnvironmentManager:
    """
    Manages environment variables.
    """

    variables: EnvironmentVars = EnvironmentVars

    @staticmethod
    def get_environment_var(key: EnvironmentVars, default: str = None):
        """
        Retrieves an environment variable.
        """
        try:
            key_val = key.value
            env_var = os.environ.get(key_val, default)
            if env_var is None:
                raise ValueError(f"Environment variable '{key_val}' not found")
            return env_var
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error occured loading environment variable '{key_val}' : {e}"
            )


class Directories(Enum):
    """
    Directories used by the application.
    """

    DATA = "data"
    CONFIG = "src/config"
    LOGS = "logs"


@dataclass(frozen=True)
class DirectoryManager:
    """
    Manages directory paths, ensuring they exist.
    """

    base_dir: Path = Path.cwd()
    directories: Directories = Directories

    @cached_property
    def directory_paths(self) -> Dict[str, Path]:
        """
        Returns a dictionary with absolute paths for all directories.
        """
        paths = {}
        for dir in self.directories:
            dir_path = self.base_dir / dir.value
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                paths[dir.name] = dir_path
            except OSError as e:
                raise RuntimeError(
                    f"Unexpected error occured while creating '{dir_path}' : {e}"
                )
        return paths

    def get_directory_path(self, directory: Directories) -> Path:
        """
        Retrieves the absolute path of a specific directory.
        """
        dir_name = directory.name
        path = self.directory_paths.get(dir_name)
        if path is None:
            raise ValueError(f"Directory '{dir_name}' not found")
        return path


@dataclass(frozen=True)
class AppConfig:
    """
    Main application configuration.
    """

    directory_manager: DirectoryManager = DirectoryManager()
    environment_manager: EnvironmentManager = EnvironmentManager()


app_config = AppConfig()
