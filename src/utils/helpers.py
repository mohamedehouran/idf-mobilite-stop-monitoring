import time
from functools import wraps
from typing import Callable, Any
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
