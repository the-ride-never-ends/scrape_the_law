import time
from typing import Any, Callable
from functools import wraps


# class DebugManager:
#     """
#     Context manager for managing debug mode in a file.
#     """
#     debug_mode = False

#     @classmethod
#     def set_debug(cls, state: bool):
#         cls.debug_mode = state



# def dec_time_sleep(seconds: int) -> Callable[..., Any]:
#     """
#     Decorator that pauses for a given number of seconds after the execution of a function based on a debug flag.
#     NOTE Python functions without returns just return 'None'.
#     """
#     def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
#         @wraps(func)
#         def wrapper(*args, **kwargs) -> Any:
#             result = func(*args, **kwargs)  # Execute the function first
#             if kwargs.get('debug', False):
#                 time.sleep(seconds)  # Sleep after the function has executed
#             return result
#         return wrapper
#     return decorator



# # def time_sleep(seconds: int, debug=False) -> None:
# #     """
# #     Pauses the program for a given number of seconds if in debug mode.
# #     """
# #     if debug:
# #         time.sleep(seconds)

# def time_sleep(seconds: int, func=None):
#     """
#     Decoratore wrapper for time.sleep()
#     It pauses for a given number of seconds after the execution of a function based on a debug flag.
#     Can also bed called as a regular function.
#     NOTE Python functions without returns just return 'None'.
#     Examples:
#     >>>  # Usage as a decorator
#     >>> @dec_time_sleep(2)
#     >>> def sample_function(a, b):
#     >>>     print(f"Adding {a} and {b}")
#     >>>     return a + b

#     >>> # Calling the decorated function
#     >>> sample_function(1, 2)

#     >>> # Usage as a standalone function
#     >>> sleep_for_2_seconds = time_sleep(2)
#     >>> sleep_for_2_seconds()
#     """
#     # Force a default of 5 if seconds isn't an int
#     seconds = seconds if isinstance(seconds, int) else 5




# def log_function_call(func=None, *, log_message=None):
#     """Decorator to log function calls. Can also be used as a regular function to log messages."""
#     if func is None:
#         # When used as a regular function
#         def log_message_direct(message):
#             print(f"Log: {message}")
#         return log_message_direct

#     # When used as a decorator
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         print(f"Calling function '{func.__name__}' with args {args} and kwargs {kwargs}")
#         if log_message:
#             print(f"Log: {log_message}")
#         return func(*args, **kwargs)
#     return wrapper

# # Usage as a decorator
# @log_function_call(log_message="Function is being called.")
# def sample_function(a, b):
#     return a + b

# sample_function(1, 2)

# # Usage as a regular function
# logger = log_function_call()
# logger("This is a standalone log message.")
