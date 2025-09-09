from functools import wraps
from typing import Any, Callable

from logger.logger import Logger

def if_not_results(message: str=None) -> Callable:
    """
    This decorator wraps the given function and checks its return value. 

    If the function returns a falsy value (None, empty list, etc.), 
    it logs a warning message and returns None. 

    Otherwise, it returns the original result.

    NOTE: The decorator uses the Logger class from the same module as the decorated function.

    Args:
        func (Callable): The function to be decorated.
        message (str, optional): A custom warning message to be logged if the function
                                 returns no results. If not provided, a default message
                                 will be used.

    Returns:
        Callable: A decorated version of the input function.

    Example:
    >>> @if_not_results
    >>> def my_function():
    >>>     # Function implementation
    >>>     return some_result
    """
    def decorator(func: Callable) -> Callable:
        """
        Create the decorator wrapper for result validation.
        
        Creates the actual decorator that wraps the target function to check
        its return value and log warnings if no results are returned.
        
        Args:
            func (Callable): The function to be wrapped with result validation.
        
        Returns:
            Callable: The wrapped function with result validation logic.
        
        Raises:
            Exception: Any exceptions from the wrapped function are propagated.
        
        Example:
            >>> @if_not_results("Custom no results message")
            >>> def my_function():
            ...     return []  # Will log warning and return None
        """
        @wraps(func)
        def wrapper(*args,**kwargs) -> Any|None:
            """
            Wrapper that validates function results and logs warnings.
            
            Executes the wrapped function and checks if it returns any results.
            If the function returns a falsy value (None, empty list, empty string, etc.),
            logs a warning message and returns None. Otherwise, returns the original result.
            
            Returns:
                Any|None: The original function result if truthy, None if falsy.
            
            Raises:
                Exception: Any exceptions from the wrapped function are propagated.
            
            Example:
                >>> # Function returns empty list - logs warning and returns None
                >>> result = wrapped_function()  # None
                >>> # Function returns data - returns data normally  
                >>> result = wrapped_function()  # [1, 2, 3]
            """
            nonlocal message
            logger = Logger(logger_name=func.__module__)

            results = func(*args,**kwargs)
            if not results:
                message = message or f"No results returned for function '{func.__name__}'"
                logger.warning(message)
                return
            else:
                return results

        return wrapper
    return decorator