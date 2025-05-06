import json
import requests
import pandas as pd
from enum import Enum
from pathlib import Path
from ast import literal_eval
from functools import lru_cache
from thefuzz import fuzz, process
from dataclasses import dataclass, field
from typing import Generator, Optional, Tuple, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config.config_validator import validate_required_vars
from src.config.logger import logger
from src.config.stop_monitoring import StopMonitoringConfig
from src.utils.helpers import catch_exceptions, FileUtils


class StopReferentialColumn(Enum):
    """
    Stop referential data main columns.
    """

    ID = "arrid"
    NAME = "arrname"
    TOWN = "arrtown"


@dataclass
class StopReferentialManager:
    """
    Manage stop referentials for the monitoring system.
    """

    sm_config: StopMonitoringConfig
    selected_towns: str
    file_path: Path = field(init=False)

    def __post_init__(self):
        self.env_manager = self.sm_config.env_manager
        self.selected_towns = (
            self.selected_towns.split(",") if self.selected_towns else None
        )

        validate_required_vars(
            {self.env_manager.variables.SELECTED_TOWNS.value: self.selected_towns}
        )

    @catch_exceptions
    def _read_referential(self) -> pd.DataFrame:
        """
        Read the referential file and return it as DataFrame.
        """
        try:
            with open(self.sm_config.referential_file_path) as file:
                data = json.load(file)
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Unexpected error occurred reading referential file : {e}")

    @catch_exceptions
    def _match_to_existing_towns(self, town: str, towns: List[str]) -> Optional[str]:
        """
        Match a town to the corresponding one in the referential.
        """
        try:
            logger.info(
                f"Matching selected town '{town}' to the corresponding one in the referential ..."
            )
            process_town = process.extractOne(
                town, towns, scorer=fuzz.ratio, score_cutoff=70
            )
            matching_town, score = process_town if process_town else (None, 0)

            if matching_town:
                logger.info(f"Successful match : {matching_town} ({score}%)")
                return matching_town
            else:
                logger.error("Matching failed : No town found with a reasonable score")
                return None
        except Exception as e:
            logger.error(f"Unexpected error occurred reading referential file : {e}")

    @catch_exceptions
    def _filter_referential(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter the referential file on selected towns.
        """
        try:
            towns = df[StopReferentialColumn.TOWN.value].drop_duplicates().to_list()
            filtered_dfs = []

            for town in self.selected_towns:
                matching_town = self._match_to_existing_towns(town, towns)

                if matching_town:
                    logger.info(
                        f"Filtering stops corresponding to '{matching_town}' ..."
                    )
                    filtered_df = df.loc[
                        df[StopReferentialColumn.TOWN.value] == matching_town
                    ]
                    filtered_dfs.append(filtered_df)
                else:
                    logger.info(f"Filtering stops starting with '{town}' ...")
                    filtered_df = df.loc[
                        df[StopReferentialColumn.TOWN.value].str.startswith(town)
                    ]
                    filtered_dfs.append(filtered_df)

            filtered_df = pd.concat(filtered_dfs, ignore_index=True)

            if filtered_df.empty:
                logger.error("Filtering failed : No stops found")
                return pd.DataFrame()
            else:
                logger.info(f"Filtering successful : {len(filtered_df)} stops found")
                return filtered_df
        except Exception as e:
            logger.error(f"Unexpected error occurred filtering referential file : {e}")

    @catch_exceptions
    def iter_stops(self) -> Generator[Tuple[int, str]]:
        """
        Yield stop ID and name from the referential.
        """
        try:
            df = self._read_referential()
            filtered_df = self._filter_referential(df)

            if not filtered_df.empty:
                logger.info("Iterating stops ...")
                for _, row in filtered_df.iterrows():
                    yield (
                        row[StopReferentialColumn.ID.value],
                        row[StopReferentialColumn.NAME.value],
                    )
        except Exception as e:
            logger.error(
                f"Unexpected error occurred iterating stops from referential : {e}"
            )


class StopMonitoringDataFormatter:
    """
    Responsible for formatting the raw responses from the IDF Mobilité Stop Monitoring API.
    """

    @catch_exceptions
    def _extract_StopMonitoringDelivery(
        self, response: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Extracts the StopMonitoringDelivery section from the response.
        """
        response = (
            response.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("StopMonitoringDelivery", [])
        )
        return response, "Succeeded" if response else "Empty response"

    @catch_exceptions
    def _extract_MonitoredStopVisit_entries(
        self, response: List[Dict[str, Any]]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extracts entries from the MonitoredStopVisit field in the response.
        """
        if not isinstance(response, list):
            return pd.DataFrame(), "Empty response"

        results = []

        for entry in response:
            visits = entry.get("MonitoredStopVisit", [])
            for visit in visits:
                results.append(visit)

        return pd.DataFrame(results), "Succeeded" if results else "Empty response"

    @catch_exceptions
    def _expand_MonitoredVehicleJourney(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, str]:
        """
        Expands the MonitoredVehicleJourney column into separate columns
        """
        target = "MonitoredVehicleJourney"

        # Ensure the presence of the target
        if target not in df.columns:
            return pd.DataFrame(), f"Missing column ({target})"

        df = pd.concat([df.drop(columns=target), df[target].apply(pd.Series)], axis=1)

        return df, "Succeeded" if not df.empty else "Empty response"

    @catch_exceptions
    def _expand_FramedVehicleJourneyRef_and_MonitoredCall(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, str]:
        """
        Expands the FramedVehicleJourneyRef and MonitoredCall columns into separate fields.
        """
        targets = ["FramedVehicleJourneyRef", "TrainNumbers", "MonitoredCall"]

        # Ensure the presence of the targets
        missing_columns = [col for col in targets if col not in df.columns]

        if missing_columns:
            return pd.DataFrame(), f"Missing column ({', '.join(missing_columns)})"

        # Concatenate expanded dfs
        df_expanded_list = [df[col].apply(pd.Series) for col in targets]
        df = pd.concat([df.drop(columns=targets)] + df_expanded_list, axis=1)
        return df, "Succeeded" if not df.empty else "Empty response"

    @catch_exceptions
    def _strip_brackets_and_nans_from_columns(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, str]:
        """
        Removes square brackets and string "nan" values from all columns.
        """
        for col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"^\[|\]$", "", regex=True)
                .str.replace(r"\bnan\b", "", regex=True)
            )
        return df, "Succeeded" if not df.empty else "Empty response"

    @catch_exceptions
    def _extract_value_from_dicts_in_columns(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, str]:
        """
        Extracts the "value" key from dictionary-like string columns.
        """

        def safe_extract(x):
            # Skip non-string values
            if not isinstance(x, str):
                return x

            # Remove leading/trailing spaces
            x = x.strip()

            # Skip non-dictionary-looking strings
            if not (x.startswith("{") and x.endswith("}")):
                return x

            try:
                # If it's a dictionary, extract the "value" key
                parsed = literal_eval(x)
                if isinstance(parsed, dict):
                    return parsed.get("value", x)
                return x
            except (ValueError, SyntaxError):
                return x

        df = df.apply(lambda col: col.apply(safe_extract))
        return df, "Succeeded" if not df.empty else "Empty response"

    @catch_exceptions
    def format_response(
        self, station_name: str, response: List[Dict[str, Any]]
    ) -> Tuple[pd.DataFrame, str]:
        """
        Formats the response data from the Stop Monitoring API for a specific station.
        """
        response, log = self._extract_StopMonitoringDelivery(response)
        df, log = self._extract_MonitoredStopVisit_entries(response)
        df, log = self._expand_MonitoredVehicleJourney(df)
        df, log = self._expand_FramedVehicleJourneyRef_and_MonitoredCall(df)

        if df.empty:
            logger.warning(f"{station_name} - Data formatting failed : {log}")
            return df

        df, log = self._strip_brackets_and_nans_from_columns(df)
        df, log = self._extract_value_from_dicts_in_columns(df)
        df.columns = df.columns.str.lower()
        logger.info(
            f"{station_name} - Data formatting completed successfully : {len(df)} rows"
        )
        return df


class StopMonitoringDataRetriever:
    """
    Responsible for orchestrating the entire process of retrieving, processing, and saving data from the IDF Mobilité Stop Monitoring API.
    """

    def __init__(
        self,
        sr_manager: StopReferentialManager,
        sm_data_formatter: StopMonitoringDataFormatter,
        file_utils: FileUtils,
    ):
        self.sr_manager = sr_manager
        self.sm_config = sr_manager.sm_config
        self.sm_data_processor = sr_manager.sm_config.data_processor
        self.sm_data_formatter = sm_data_formatter
        self.file_utils = file_utils

    @catch_exceptions
    @lru_cache(maxsize=100)
    def _fetch_stop_point_data(
        self, stop_point_id: int, station_name: str
    ) -> Dict[str, Any]:
        """
        Sends a GET request to retrieve stop point data from the API.
        """
        url = self.sm_config.get_request_url(stop_point_id)
        response = requests.get(url, headers=self.sm_config.headers)
        response.raise_for_status()
        response = response.json()
        logger.info(
            f"{station_name} - Data retrieval successful : {len(response)} records"
        )
        return response

    @catch_exceptions
    def _process_response(self, stop_point_name: str, response: Dict[str, Any]) -> bool:
        """
        Processes the response for a given stop point and saves the results.
        """
        # Save raw file
        self.file_utils.save_to_json(response, stop_point_name, is_raw=True)

        # Format response
        df = self.sm_data_formatter.format_response(stop_point_name, response)
        is_processed = not df.empty

        # Save processed file
        if is_processed:
            self.file_utils.save_to_parquet(
                df, self.sm_config.output_filename, to_append=True, clean_filename=False
            )

        return is_processed

    @catch_exceptions
    def _retrieve_and_process_responses(self) -> pd.DataFrame:
        """
        Retrieves and processes responses from multiple stop points in parallel.
        """
        responses_processed = 0

        logger.info(
            f"Starting parallel retrieval with {self.sm_config.data_processor.max_workers} workers ..."
        )
        with ThreadPoolExecutor(
            max_workers=self.sm_config.data_processor.max_workers
        ) as executor:
            futures = {
                executor.submit(
                    self._fetch_stop_point_data, stop_point_id, stop_point_name
                ): stop_point_name
                for stop_point_id, stop_point_name in self.sr_manager.iter_stops()
            }

            requests = len(futures)

            for future in as_completed(futures):
                stop_point_name = futures[future]

                try:
                    response = future.result(timeout=30)
                    responses_processed += self._process_response(
                        stop_point_name, response
                    )

                except Exception as e:
                    logger.error(
                        f"Unexpected error occurred while processing {stop_point_name} : {e}"
                    )
                    raise

        logger.info(
            f"Parallel retrieval completed : {requests} requests sent, {responses_processed} responses processed"
        )

    @catch_exceptions
    def execute_retrieval_workflow(self) -> pd.DataFrame:
        """
        Executes the entire workflow for retrieving and processing arrival times.
        """
        logger.info("Executing stop monitoring data retrieval workflow ...")
        self._retrieve_and_process_responses()
        logger.info("Stop monitoring data retrieval workflow completed successfully")
