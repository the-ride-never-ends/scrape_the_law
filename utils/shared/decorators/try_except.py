from functools import wraps
import logging
from typing import Any, Callable

from logger import Logger

def try_except(exception: list=[Exception],
               raise_exception: bool=False,
               retries: int=0,
               logger: logging.Logger=None,
               ) -> Callable:
    """
    A decorator that wraps a function in a try-except block with optional retries and exception raising.

    This decorator allows you to automatically handle exceptions for a function,
    with the ability to specify the number of retry attempts and whether to
    ultimately raise the exception or not.

    NOTE: 'Exception' is automatically added to the exception argument list if not specified.

    Args:
        exception (list): A tuple of exception types to catch. Defaults to [Exception].
        raise_exception (bool): If True, raises the caught exception after all retries
                                have been exhausted. If False, suppresses the exception.
                                Defaults to False.
        retries (int): The number of times to retry the function if an exception occurs.
                       If None, the function will only be attempted once. Defaults to None.
        logger (logging.Logger): A logger instance. Defaults to None.

    Returns:
        function: A decorated function that implements the try-except logic.

    Example:
    >>> @try_except(exception=[ValueError, TypeError], raise_exception=True, retries=3)
    >>> def test_func(x):
    >>>     return x / 0 
    >>> test_func(-1)
    ERROR:__main__:ValueError exception in 'test_func': x cannot be negative
    Retrying (0/3)...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:

            # Initialize Logger and other variables.
            # NOTE See: https://stackoverflow.com/questions/1261875/what-does-nonlocal-do-in-python-3
            nonlocal logger
            logger = logger or Logger(logger_name=func.__module__)
            attempts = 0

            # Since we don't want any uncaught exceptions
            # We add in Exception to the input exception list 
            # if it's not specified in it.
            exception_list = list(exception)
            if Exception not in exception_list:
                exception_list.extend(Exception)
            exception_tuple = tuple(exception_list)

            while attempts <= retries:
                # Try the function.
                try:
                    return func(*args, **kwargs)
                except exception_tuple as e:
                    # Define error variables
                    e: Exception
                    error_name: str = e.__class__.__name__
                    func_name = func.__name__
                    error_message = f"{error_name} exception in '{func_name}': {e}"
                    retry_message = f"Retrying ({attempts}/{retries})..."

                    # If no retries are specified, log the error and raise it if requested.
                    if retries <= 0: 
                        logger.exception(error_message)
                        if raise_exception:
                            raise e
                    else:
                        # On first attempt, print the error and retry message
                        if attempts <= 0: 
                            print(error_message)
                            print(retry_message)
                        # On subsequent attempts, print the retry message.
                        elif attempts > 0 and attempts < retries: 
                            print(retry_message)
                        # On the final attempt, log the error and raise it if requested.
                        else: 
                            print(f"Function '{func_name}' errored after {attempts + 1} retries.")
                            logger.exception(f"{error_message}\nretries: {attempts + 1}")
                            if raise_exception:
                                raise e
                        attempts += 1
        return wrapper
    return decorator
