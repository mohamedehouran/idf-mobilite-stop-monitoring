import click
from src.config.app import app_config
from src.config.stop_monitoring import (
    StopMonitoringDataProcessorConfig,
    StopMonitoringConfig,
)
from src.utils.data_retriever import (
    StopReferentialManager,
    StopMonitoringDataFormatter,
    StopMonitoringDataRetriever,
)
from src.utils.helpers import catch_exceptions, FileUtils


env_manager = app_config.environment_manager


@click.command()
@click.option(
    "--selected_towns",
    type=str,
    required=True,
    default=env_manager.get_environment_var(env_manager.variables.SELECTED_TOWNS),
    show_default=True,
    prompt="Enter the town(s) selected for stop monitoring (separated by comma)",
)
@catch_exceptions
def main(selected_towns: str):
    # Initialize common workflow components
    file_utils = FileUtils(app_config)
    sm_config = StopMonitoringConfig(
        data_processor=StopMonitoringDataProcessorConfig(app_config)
    )
    sr_manager = StopReferentialManager(
        sm_config=sm_config, selected_towns=selected_towns
    )

    # Initilialize workflow
    sm_data_retriever = StopMonitoringDataRetriever(
        sr_manager=sr_manager,
        sm_data_formatter=StopMonitoringDataFormatter(),
        file_utils=file_utils,
    )

    # Execute the workflow
    sm_data_retriever.execute_retrieval_workflow()


if __name__ == "__main__":
    main()
