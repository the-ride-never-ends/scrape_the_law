from functools import wraps
import time
from typing import Any, Callable

from logger.logger import Logger

def get_exec_time(func: Callable) -> Any:
    """
    Decorator to calculate how long a function takes to execute.

    Examples:
    >>> @get_time
    >>> def factorial(num):
    >>>     time.sleep(2)
    >>>     print(math.factorial(num))
    >>> factorial(10)
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that measures and logs execution time.
        
        Wraps the original function to measure its execution time from start
        to finish. Logs the total execution time using the configured logger
        and returns the original function's result.
        
        Returns:
            Any: The result returned by the wrapped function.
        
        Raises:
            Exception: Any exceptions raised by the wrapped function are propagated.
        
        Example:
            >>> @get_exec_time
            >>> def slow_function():
            ...     time.sleep(2)
            ...     return "done"
            >>> result = slow_function()  # Logs: "Total execution time for 'slow_function': 2.00"
        """
        # Define the logger.
        logger = Logger(logger_name=func.__module__)

        # Log start time.
        begin = time.time()
        
        # Execute the function.
        result = func(*args, **kwargs)

        # Log end time.
        end = time.time()
        logger.info(f"Total execution time for '{func.__name__}':  {end - begin}")
        return result
    return wrapper


