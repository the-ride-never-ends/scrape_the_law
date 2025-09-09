from typing import Any, Callable, Coroutine

import pandas as pd

async def create_tasks_list(inputs: Any, func: Callable, enum: bool, *args, **kwargs) -> list[Coroutine[Any, Any, Any]]:
    """
    Create a list of coroutines from inputs and a function.
    
    Takes various input types (list, dict, DataFrame) and creates coroutines by applying
    the provided function to each element. Supports enumeration to include indices.
    
    Args:
        inputs (Any): Input data - supports list, set, tuple, dict, or pandas DataFrame.
        func (Callable): Function to apply to each input element. Should return a coroutine.
        enum (bool): Whether to include enumeration index as first argument to func.
        *args: Additional positional arguments to pass to func.
        **kwargs: Additional keyword arguments to pass to func.
    
    Returns:
        list[Coroutine[Any, Any, Any]]: List of coroutines ready for execution with
            asyncio.gather() or similar async execution methods.
    
    Raises:
        ValueError: If inputs type is not supported (not list, set, tuple, dict, or DataFrame).
    
    Example:
        >>> async def process_item(item):
        ...     return item * 2
        >>> tasks = await create_tasks_list([1, 2, 3], process_item, False)
        >>> results = await asyncio.gather(*tasks)
        >>> print(results)
        [2, 4, 6]
        >>> # With enumeration:
        >>> async def process_with_index(idx, item):
        ...     return f"{idx}: {item}"
        >>> tasks = await create_tasks_list(['a', 'b'], process_with_index, True)
    """

    if isinstance(inputs, (list,set,tuple)):
        if enum:
            return [func(idx, inp, *args, **kwargs) for idx, inp in enumerate(inputs)]
        else:
            return [func(inp, *args, **kwargs) for inp in inputs]

    elif isinstance(inputs, dict):
        if enum:
            return [func(idx, (key, value), *args, **kwargs) for idx, (key, value) in enumerate(inputs.items())]
        else:
            return [func((key, value), *args, **kwargs) for key, value in inputs.items()]

    elif isinstance(inputs, pd.DataFrame):
        if enum:
            return [func(idx, row, *args, **kwargs) for idx, row in enumerate(inputs.itertuples())]
        else:
            return [func(row, *args, **kwargs) for row in inputs.itertuples()]

    else:
        raise ValueError(f"Argument 'inputs' has an unsupported type '{type(inputs)}'")