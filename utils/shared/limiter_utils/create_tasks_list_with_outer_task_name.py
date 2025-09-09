import asyncio
from typing import Any, Coroutine

import pandas as pd

async def create_tasks_list_with_outer_task_name(inputs: Any, func: Coroutine, enum: bool, outer_task_name: str, *args, **kwargs) -> list[asyncio.Task]:
    """
    Create a list of named asyncio Tasks from inputs and a coroutine function.
    
    Similar to create_tasks_list but wraps each coroutine in an asyncio.Task with
    a specified name for better debugging and task management. Supports various
    input types and optional enumeration.
    
    Args:
        inputs (Any): Input data - supports list, set, tuple, dict, or pandas DataFrame.
        func (Coroutine): Coroutine function to apply to each input element.
        enum (bool): Whether to include enumeration index as first argument to func.
        outer_task_name (str): Name to assign to each created Task for identification.
        *args: Additional positional arguments to pass to func.
        **kwargs: Additional keyword arguments to pass to func.
    
    Returns:
        list[asyncio.Task]: List of named asyncio Tasks ready for execution.
    
    Raises:
        ValueError: If inputs type is not supported (not list, set, tuple, dict, or DataFrame).
    
    Example:
        >>> async def process_item(item):
        ...     return item * 2
        >>> tasks = await create_tasks_list_with_outer_task_name(
        ...     [1, 2, 3], process_item, False, "multiply_task"
        ... )
        >>> for task in tasks:
        ...     print(task.get_name())
        multiply_task
        multiply_task  
        multiply_task
        >>> results = await asyncio.gather(*tasks)
    """

    if isinstance(inputs, (list,set,tuple)):
        if enum:
            return [
                asyncio.create_task(
                    func(idx, inp, *args, **kwargs), 
                    name=outer_task_name
                ) for idx, inp in enumerate(inputs)
            ]
        else:
            return [
                asyncio.create_task(
                    func(inp, *args, **kwargs), 
                    name=outer_task_name
                ) for inp in inputs
            ]

    elif isinstance(inputs, dict):
        if enum:
            return [
                asyncio.create_task(
                    func(idx, (key, value), *args, **kwargs), 
                    name=outer_task_name
                    ) for idx, (key, value) in enumerate(inputs.items())
                ]
        else:
            return [
                asyncio.create_task(
                    func((key, value), *args, **kwargs),
                    name=outer_task_name,
                    ) for (key, value) in inputs.items()
                ]

    elif isinstance(inputs, pd.DataFrame):
        if enum:
            return [
                asyncio.create_task(
                    func(idx, row, *args, **kwargs), 
                    name=outer_task_name
                ) for idx, row in enumerate(inputs.itertuples())
            ]
        else:
            return [
                asyncio.create_task(
                    func(row, *args, **kwargs), 
                    name=outer_task_name
                ) for row in inputs.itertuples()
            ]

    else:
        raise ValueError(f"Argument 'inputs' has an unsupported type '{type(inputs)}'")