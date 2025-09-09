import asyncio
from functools import wraps
import time
from typing import Any, Callable, Coroutine

from logger.logger import Logger

def adjust_wait_time_for_execution(wait_in_seconds: float=5) -> Callable[..., Any]:
    """
    Decorator to adjust wait time based on actual function execution time.
    
    Creates a decorator that measures function execution time and adjusts
    a predefined wait time accordingly. If a function takes 2 seconds to
    execute and the wait time is 5 seconds, only 3 seconds of additional
    waiting will occur. Useful for respecting rate limits while optimizing
    for execution time.
    
    Args:
        wait_in_seconds (float): Initial wait time in seconds that will be
            adjusted based on execution time. Defaults to 5.
    
    Returns:
        Callable: Decorator function that wraps the target function with
            execution-adjusted waiting.
    
    Raises:
        ValueError: If wait_in_seconds is negative.
    
    Example:
        >>> @adjust_wait_time_for_execution(wait_in_seconds=3.0)
        >>> def api_call():
        ...     return requests.get("https://api.example.com")
        >>> # If api_call takes 1 second, will wait additional 2 seconds
    """
    """
    Adjust a sleep waiting period to account for the clock time taken to execute a synchronous function.
    Useful for optimizing waiting periods based on a reference value e.g. a robots.txt delay.
    """
    def decorator(func: Callable) -> Callable:
        """
        Create the synchronous decorator wrapper for execution-adjusted waiting.
        
        Wraps the target function to measure its execution time and adjust
        the waiting period accordingly.
        
        Args:
            func (Callable): The function to be wrapped with execution-adjusted waiting.
        
        Returns:
            Callable: The wrapped function with execution timing and adjusted waiting.
        
        Raises:
            Exception: Any exceptions from the wrapped function are propagated.
        
        Example:
            >>> @adjust_wait_time_for_execution(3.0)
            >>> def my_function():
            ...     return "result"
        """
        @wraps(func)
        def wrapper(*args,**kwargs) -> Any|None:
            """
            Synchronous wrapper that executes function and adjusts wait time.
            
            Measures the execution time of the wrapped function and sleeps for
            the remaining time to achieve the target wait period. Logs timing
            information for debugging and optimization.
            
            Returns:
                Any|None: The result returned by the wrapped function.
            
            Raises:
                Exception: Any exceptions from the wrapped function are propagated.
            
            Example:
                >>> result = wrapped_function(arg1, arg2)
                >>> # Logs execution time and waits for adjusted duration
            """
            # Initialize nonlocal and logger
            nonlocal wait_in_seconds
            logger = Logger(logger_name=func.__module__)

            # Wait for function to run, clock it's runtime, then subtract that from wait_in_seconds.
            start = time.time()
            result = func(*args,**kwargs)
            end = time.time() - start
            wait_in_seconds -= end

            # Pause for the duration of the adjusted wait_in_seconds, then return the function's result.
            logger.info(f"Execution time for function '{func.__name__}' took {end} seconds to execute.\nWait time is now '{wait_in_seconds}' seconds.")
            if wait_in_seconds > 0:
                time.sleep(wait_in_seconds)
            return result

        return wrapper
    return decorator


def async_adjust_wait_time_for_execution(wait_in_seconds: float=5) -> Coroutine[None, None, Any]:
    """
    Async decorator to adjust wait time based on actual function execution time.
    
    Creates an async decorator that measures async function execution time and
    adjusts a predefined wait time accordingly. Provides the same functionality
    as adjust_wait_time_for_execution but for async functions using asyncio.sleep.
    
    Args:
        wait_in_seconds (float): Initial wait time in seconds that will be
            adjusted based on execution time. Defaults to 5.
    
    Returns:
        Coroutine: Async decorator function that wraps the target coroutine with
            execution-adjusted waiting.
    
    Raises:
        ValueError: If wait_in_seconds is negative.
    
    Example:
        >>> @async_adjust_wait_time_for_execution(wait_in_seconds=3.0)
        >>> async def async_api_call():
        ...     return await aiohttp.get("https://api.example.com")
        >>> # If api_call takes 1 second, will wait additional 2 seconds
    """
    """
    Adjust a sleep waiting period to account for the clock time taken to execute a synchronous function.
    Useful for optimizing waiting periods based on a reference value e.g. a robots.txt delay.
    """
    def decorator(func: Coroutine) -> Coroutine:
        """
        Create the async decorator wrapper for execution-adjusted waiting.
        
        Wraps the target async function to measure its execution time and
        adjust the waiting period accordingly using asyncio.sleep.
        
        Args:
            func (Coroutine): The async function to be wrapped with execution-adjusted waiting.
        
        Returns:
            Coroutine: The wrapped async function with execution timing and adjusted waiting.
        
        Raises:
            Exception: Any exceptions from the wrapped function are propagated.
        
        Example:
            >>> @async_adjust_wait_time_for_execution(3.0)
            >>> async def my_async_function():
            ...     return await some_operation()
        """
        @wraps(func)
        async def wrapper(*args,**kwargs) -> Any|None:
            """
            Async wrapper that executes function and adjusts wait time.
            
            Measures the execution time of the wrapped async function and sleeps
            for the remaining time using asyncio.sleep to achieve the target wait
            period. Logs timing information and ensures non-negative wait times.
            
            Returns:
                Any|None: The result returned by the wrapped async function.
            
            Raises:
                Exception: Any exceptions from the wrapped function are propagated.
            
            Example:
                >>> result = await wrapped_function(arg1, arg2)
                >>> # Logs execution time and waits for adjusted duration
            """
            # Initialize nonlocal and logger
            nonlocal wait_in_seconds
            logger = Logger(logger_name=func.__module__)

            # Wait for function to run, clock it's runtime, then subtract that from wait_in_seconds.
            start = time.time()
            result = await func(*args,**kwargs)
            timespan = time.time() - start
            wait_in_seconds -= timespan
            if wait_in_seconds < 0:
                wait_in_seconds = 0

            # Pause for the duration of the adjusted wait_in_seconds, then return the function's result.
            logger.info(f"Execution time for function '{func.__name__}' took {timespan} seconds to execute.\nWait time is now '{wait_in_seconds}' seconds.")
            if wait_in_seconds > 0:
                asyncio.sleep(wait_in_seconds)
            return result

        return wrapper
    return decorator

