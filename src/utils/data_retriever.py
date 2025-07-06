import time
import requests
import pandas as pd
from enum import Enum
from ast import literal_eval
from functools import lru_cache
from thefuzz import fuzz, process
from dataclasses import dataclass, field
from typing import Generator, Optional, Tuple, Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config.logger import logger
from src.config.stop_monitoring import StopMonitoringConfig, StopReferentialConfig
from src.utils.helpers import catch_exceptions


class StopReferentialColumn(Enum):
    """
    Stop referential data main columns.
    """

    ID = "arrid"
    NAME = "arrname"
    TOWN = "arrtown"


class StopReferentialManager:
    """
    Responsible for managing the stop referential data, including reading, filtering, and iterating over stops.
    """

    def __init__(
        self, config: StopReferentialConfig, sm_config: StopMonitoringConfig
    ) -> None:
        self.config = config
        self.selected_towns = sm_config.selected_towns

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
            logger.error(f"Unexpected error occurred matching to existing towns : {e}")

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
            df = self.config.load_referential()
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


@dataclass
class StopMonitoringDataRetrieverResult:
    """
    Represents the result of the Stop Monitoring data retrieval process.
    """

    execution_time: float
    processed_file_path: str
    total_processed: str
    total_successful: str
    total_failed: str
    success_rate: str = field(init=False)
    failure_rate: str = field(init=False)
    status: str = field(init=False)

    def __post_init__(self) -> None:
        self.success_rate = self._compute_ratio(self.total_successful)
        self.failure_rate = self._compute_ratio(self.total_failed)
        self.status = self._get_status()

    def _compute_ratio(self, value: str) -> str:
        """
        Compute the ratio of a given value over the total processed count.
        """
        return (
            f"{(int(value) / int(self.total_processed) * 100):.2f}%"
            if self.total_processed
            else "0.00%"
        )

    def _get_status(self) -> str:
        """
        Determine the overall status based on the success rate.
        """
        return (
            "SUCCESS"
            if self.success_rate == "100%"
            else "PARTIAL_SUCCESS"
            if self.success_rate != "0.00%"
            else "FAILED"
        )


class StopMonitoringDataRetriever:
    """
    Responsible for orchestrating the entire process of retrieving, processing, and saving data from the IDF Mobilité Stop Monitoring API.
    """

    def __init__(
        self,
        sm_config: StopMonitoringConfig,
        sm_data_formatter: StopMonitoringDataFormatter,
        sr_manager: StopReferentialManager,
    ) -> None:
        self.sm_config = sm_config
        self.sm_data_formatter = sm_data_formatter
        self.sr_manager = sr_manager

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
    def _save_processed_df(self, df: pd.DataFrame) -> None:
        """
        Saves the processed DataFrame to a CSV file, appending if the file already exists.
        """
        write_header = not self.sm_config.processed_file_path.exists()
        df.to_csv(
            self.sm_config.processed_file_path,
            mode="a",
            header=write_header,
            index=False,
        )

    @catch_exceptions
    def _process_response(self, stop_point_name: str, response: Dict[str, Any]) -> bool:
        """
        Processes the response for a given stop point and saves the results.
        """
        # Format response
        df = self.sm_data_formatter.format_response(stop_point_name, response)
        is_processed = not df.empty

        # Save processed file
        if is_processed:
            self._save_processed_df(df)

        return is_processed

    @catch_exceptions
    def execute_retrieval_workflow(self) -> pd.DataFrame:
        """
        Executes the retrieval workflow for stop monitoring data, processing each stop point in parallel.
        """
        total_processed = 0
        total_successful = 0
        time_start = time.time()

        logger.info(
            f"Starting stop monitoring retrieval for '{"', '".join(self.sm_config.selected_towns)}' ..."
        )
        with ThreadPoolExecutor(max_workers=self.sm_config.max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_stop_point_data, stop_point_id, stop_point_name
                ): stop_point_name
                for stop_point_id, stop_point_name in self.sr_manager.iter_stops()
            }

            for future in as_completed(futures):
                stop_point_name = futures[future]

                try:
                    response = future.result(timeout=30)
                    total_processed += 1
                    total_successful += self._process_response(
                        stop_point_name, response
                    )

                except Exception as e:
                    logger.error(
                        f"Unexpected error occurred processing '{stop_point_name}' : {e}"
                    )
                    raise

        elapsed_time = time.time() - time_start

        logger.info(
            f"Retrieval workflow completed in {elapsed_time:.2f} seconds : {total_processed} requests processed"
        )
        return StopMonitoringDataRetrieverResult(
            execution_time=str(elapsed_time),
            processed_file_path=str(self.sm_config.processed_file_path),
            total_processed=str(total_processed),
            total_successful=str(total_successful),
            total_failed=str(total_processed - total_successful),
        )
