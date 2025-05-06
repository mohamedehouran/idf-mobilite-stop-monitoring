import re
import json
import time
import pandas as pd
from typing import Dict
from pathlib import Path
from functools import wraps
from typing import Callable, Any
from src.config.app import AppConfig
from src.config.logger import logger


def catch_exceptions(function: Callable) -> Callable:
    """
    Handles exceptions that occur during the execution of a function.
    """

    @wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.time()
        logger.debug(f"Executing {function.__name__}...")

        try:
            result = function(*args, **kwargs)
            duration = time.time() - start_time
            logger.debug(f"{function.__name__} ran successfully in {duration:.2f} sec")
            return result

        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                f"Unexpected error occured in {function.__name__}  after {duration:.2f} sec : {type(e).__name__} - {e}"
            )
            raise

    return wrapper


class FileUtils:
    """
    Utility class responsible for handling and saving file.
    """

    def __init__(self, app_config: AppConfig):
        self.dir_manager = app_config.directory_manager

    @catch_exceptions
    def _clean_text(self, input_text: str) -> str:
        """
        Removes non-alphabetic characters from input text.
        """
        return re.sub(r"[^a-zA-ZÀ-ÿ\s]", "", input_text)

    @catch_exceptions
    def _get_file_path(
        self, filename: str, is_raw: bool = False, clean_filename=True
    ) -> str:
        """
        Return a file path
        """
        if clean_filename:
            filename = self._clean_text(filename.lower())

        dir = (
            self.dir_manager.get_directory_path(self.dir_manager.directories.RAW_DATA)
            if is_raw
            else self.dir_manager.get_directory_path(
                self.dir_manager.directories.PROCESSED_DATA
            )
        )
        return Path(dir) / filename

    @catch_exceptions
    def save_to_json(
        self,
        data: Dict[str, str],
        filename: str,
        is_raw: bool = False,
        clean_filename=True,
    ) -> None:
        """
        Save a JSON file.
        """
        file_path = self._get_file_path(filename, is_raw, clean_filename).with_suffix(
            ".json"
        )

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    @catch_exceptions
    def save_to_parquet(
        self,
        df: pd.DataFrame,
        filename: str,
        to_append: bool = False,
        is_raw: bool = False,
        clean_filename=True,
    ) -> None:
        """
        Save a DataFrame to a CSV file.
        """
        file_path = self._get_file_path(filename, is_raw, clean_filename).with_suffix(
            ".parquet"
        )

        # Load old DataFrame for clean stacking
        if to_append and file_path.exists():
            old_df = pd.read_parquet(file_path)
            df = pd.concat([old_df, df], ignore_index=True, sort=False)

        df.to_parquet(file_path, index=False)

    @catch_exceptions
    def get_processed_data_path(self, filename: str, file_type: str) -> Path:
        """ """
        return (
            self.dir_manager.get_directory_path(
                self.dir_manager.directories.PROCESSED_DATA
            )
            / f"{filename}.{file_type}"
        )
