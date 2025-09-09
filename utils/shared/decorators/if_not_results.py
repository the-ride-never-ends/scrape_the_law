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
    Decorator function.
    
    TODO: Add proper description.
    
    Args:
        func (Callable): Description needed.
    
    Returns:
        Callable: Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> decorator()
    """
        @wraps(func)
        def wrapper(*args,**kwargs) -> Any|None:

    """
    Wrapper function.
    
    TODO: Add proper description.
    
    Returns:
        Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> wrapper()
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