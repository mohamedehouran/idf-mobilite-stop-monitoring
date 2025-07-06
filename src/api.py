from dataclasses import asdict
from fastapi.responses import FileResponse
from fastapi import FastAPI, HTTPException, Query
from src.config.app import app_config
from src.config.stop_monitoring import StopMonitoringConfig, StopReferentialConfig
from src.utils.data_retriever import (
    StopReferentialManager,
    StopMonitoringDataFormatter,
    StopMonitoringDataRetriever,
)


app = FastAPI(
    title="🌍 IDF Mobilité Stop Monitoring",
    description=(
        "IDF Mobilité Stop Monitoring is a powerful Python-based solution for retrieving and processing public transit arrival time data from the IDF Mobilité API."
        "Designed for efficiency and reliability, it supports multi-stop processing and implements smart data formatting to ensure high-quality outputs."
    ),
    version="0.1.0",
    github="https://github.com/mohamedehouran/idf-mobilite-stop-monitoring/",
)


@app.post(
    "/stop-monitoring/",
    summary="Retrieves Stop Monitoring Data",
    description=(
        "This endpoint retrieves real-time arrival times for one or more stop points from the IDF Mobilité API."
        "You can specify one or multiple towns, and the service will identify their stop points, query the API, process the results, and return a formatted CSV file."
    ),
    response_description="CSV file containing real-time arrival information for the requested towns",
    tags=["Stop Monitoring"],
)
async def retrieve_stop_monitoring_data(
    selected_towns: str = Query(
        example="Paris,Versailles",
        title="Selected towns",
        description=(
            "Comma-separated list of towns for which to retrieve stop monitoring data"
        ),
    ),
):
    try:
        # Initialize components
        sm_config = StopMonitoringConfig(
            app_config=app_config, selected_towns=selected_towns
        )
        sm_data_retriever = StopMonitoringDataRetriever(
            sm_config=sm_config,
            sm_data_formatter=StopMonitoringDataFormatter(),
            sr_manager=StopReferentialManager(
                config=StopReferentialConfig(app_config), sm_config=sm_config
            ),
        )

        # Execute the workflow
        result = sm_data_retriever.execute_retrieval_workflow()
        return FileResponse(
            path=result.processed_file_path,
            media_type="text/csv",
            headers=asdict(result),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error occured fetching stop monitoring data : {str(e)}",
        )
