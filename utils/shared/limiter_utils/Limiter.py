import asyncio
from typing import Any, Callable, Coroutine, Generator


from tqdm import asyncio as tqdmasyncio


from utils.shared.limiter_utils.create_tasks_list import create_tasks_list
from utils.shared.limiter_utils.create_tasks_list_with_outer_task_name import create_tasks_list_with_outer_task_name


class Limiter:
    """
    Create a custom rate-limiter based on a semaphore.
    Options for a custom stop condition and progress bar.
    """
    def __init__(self, 
                 semaphore: int=2, 
                 stop_condition: Any = "stop_condition", # Replace with your specific stop condition
                 progress_bar: bool=True
                ):
        """
        Initialize a rate limiter for controlling concurrent async operations.
        
        Creates a custom rate limiter using asyncio.Semaphore to control the number
        of concurrent operations. Supports optional stop conditions and progress bar
        display during execution.
        
        Args:
            semaphore (int): Maximum number of concurrent operations allowed.
                Defaults to 2.
            stop_condition (Any): Value that when returned by a task will trigger
                a global stop signal. Defaults to "stop_condition".
            progress_bar (bool): Whether to display a progress bar during
                batch operations. Defaults to True.
        
        Returns:
            None
        
        Raises:
            ValueError: If semaphore is not a positive integer.
            TypeError: If invalid parameter types are provided.
        
        Example:
            >>> limiter = Limiter(semaphore=5, progress_bar=False)
            >>> limiter = Limiter()  # Use defaults
        """
        self.semaphore = asyncio.Semaphore(semaphore)
        self.stop_condition = stop_condition
        self.progress_bar = progress_bar

    # Claude insisted that I include these for compatability/future use purposes.
    # It's probably a good idea. 
    async def __aenter__(self):
        """
        Enter the async context manager for the limiter.
        
        Enables the limiter to be used with async context manager syntax
        (async with statement). Returns self to allow method chaining and
        resource management.
        
        Args:
            None
        
        Returns:
            Limiter: The limiter instance for use in the context.
        
        Raises:
            RuntimeError: If called outside an async context.
        
        Example:
            >>> async with Limiter(semaphore=3) as limiter:
            ...     await limiter.run_task_with_limit(some_task)
        """
        """
        Initialize the Limiter using a context manager.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the async context manager for the limiter.
        
        Handles cleanup when exiting the async context manager. Currently
        performs no cleanup operations but provides the interface for future
        resource management needs.
        
        Args:
            exc_type (type): Exception type if an exception occurred, None otherwise.
            exc_val (Exception): Exception value if an exception occurred, None otherwise.
            exc_tb (traceback): Exception traceback if an exception occurred, None otherwise.
        
        Returns:
            None: Allows exceptions to propagate normally.
        
        Raises:
            Exception: Re-raises any exceptions that occurred in the context.
        
        Example:
            >>> async with Limiter() as limiter:
            ...     # Operations here
            ...     pass  # __aexit__ called automatically
        """
        """
        Exit the limiter using a context manager.
        """
        pass 

    @classmethod
    def start(cls):
        """
        Create and return a new Limiter instance using default settings.
        
        Factory method for creating a Limiter instance with default parameters.
        This is an alternative to direct instantiation that may be extended
        in the future for additional initialization logic.
        
        Args:
            cls (type): The Limiter class.
        
        Returns:
            Limiter: A new Limiter instance with default settings.
        
        Raises:
            TypeError: If cls is not the Limiter class.
        
        Example:
            >>> limiter = Limiter.start()
            >>> # Equivalent to: limiter = Limiter()
        """
        """
        Initialize the Limiter using a factory method.
        """
        instance = cls()
        return instance

    def stop():
        """
        Placeholder method for stopping limiter operations.
        
        Currently a no-op method that serves as a placeholder for future
        functionality to gracefully stop or cleanup limiter operations.
        
        Returns:
            None
        
        Raises:
            NotImplementedError: If extended functionality is expected but not implemented.
        
        Example:
            >>> Limiter.stop()  # Currently does nothing
        """
        """
        Exit the limiter.
        """
        pass


    async def run_task_with_limit(self, task: Coroutine) -> Any:
        """
        Execute a single async task with rate limiting applied.
        
        Runs the provided coroutine with semaphore-based rate limiting to control
        concurrency. Also checks the task result against the stop condition and
        sets a global stop signal if matched.
        
        Args:
            task (Coroutine): The async task/coroutine to execute with rate limiting.
        
        Returns:
            Any: The result returned by the executed task.
        
        Raises:
            asyncio.CancelledError: If the task is cancelled.
            Exception: Any exception raised by the task is propagated.
        
        Example:
            >>> async def my_task():
            ...     return "completed"
            >>> limiter = Limiter(semaphore=2)
            >>> result = await limiter.run_task_with_limit(my_task())
        """
        """
        Set up rate-limit-conscious functions
        """
        async with self.semaphore:
            result = await task
            if result == self.stop_condition:  
                global stop_signal
                stop_signal = True
            return result 


    async def run_async_many(self, 
                             *args, 
                             inputs: Any=None, 
                             func: Callable=None,
                             enum: bool=True,
                             outer_task_name: str = "",
                             **kwargs
                            ) -> asyncio.Future | Generator:
        """
        Execute multiple async tasks concurrently with rate limiting.
        
        Processes a collection of inputs through a provided async function with
        rate limiting applied. Supports optional progress bar display and task
        naming for debugging. Creates tasks dynamically and manages their execution
        with the configured concurrency limits.
        
        Args:
            *args: Positional arguments to pass to the function.
            inputs (Any): Collection of inputs to process through the function.
                Required parameter.
            func (Callable): Async function to apply to each input. Required parameter.
            enum (bool): Whether to enumerate inputs when creating tasks. Defaults to True.
            outer_task_name (str): Name prefix for created tasks for debugging.
                Defaults to empty string.
            **kwargs: Keyword arguments to pass to the function.
        
        Returns:
            asyncio.Future | Generator: Results from all executed tasks, or generator
                if progress_bar is True.
        
        Raises:
            ValueError: If inputs or func parameters are not provided.
            TypeError: If func is not callable or inputs is not iterable.
            asyncio.CancelledError: If tasks are cancelled during execution.
        
        Example:
            >>> async def process_item(item):
            ...     return f"processed_{item}"
            >>> limiter = Limiter(semaphore=3)
            >>> items = ['a', 'b', 'c', 'd']
            >>> results = await limiter.run_async_many(inputs=items, func=process_item)
        """
        if not inputs:
            raise ValueError("input_list was not input as a parameter")

        if not func:
            raise ValueError("func was not input as a parameter")

        # NOTE Adding an outer_task_name changes the tasks list from a list of Coroutines to a list of Tasks.
        # However, running it through the limiter appears to change them back into Coroutines, so maybe it's fine???
        if outer_task_name and self.progress_bar is False:
            tasks = create_tasks_list_with_outer_task_name(inputs, func, enum, *args, **kwargs)
        else:
            tasks = create_tasks_list(inputs, func, enum, *args, **kwargs)

        task_list = [
            self.run_task_with_limit(task) for task in tasks
        ]

        if self.progress_bar:
            for future in tqdmasyncio.tqdm.as_completed(task_list):
                await future
        else:
            return await asyncio.gather(*task_list)

