

try:
    # NOTE Since this function might be used in code with or without the Pandas library,
    # we do this to make sure we can still use the function.
    import pandas as pd

    def _check_if_pandas_df(args: pd.DataFrame|tuple|dict|list[str]) -> list[str]:
    """
    Extract column names from a pandas DataFrame.
    
    Args:
        args (pd.DataFrame|tuple|dict|list[str]): Input that should be a pandas DataFrame.
    
    Returns:
        list[str]: List of column names from the DataFrame.
    
    Raises:
        AttributeError: If args doesn't have columns attribute.
    
    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'a': [1], 'b': [2]})
        >>> _check_if_pandas_df(df)
        ['a', 'b']
    """
    return args.columns.to_list()

except ImportError:

    def _check_if_pandas_df(args: tuple|dict|list[str]) -> tuple|dict|list[str]:
    """
    Fallback function when pandas is not available.
    
    Args:
        args (tuple|dict|list[str]): Input args to return unchanged.
    
    Returns:
        tuple|dict|list[str]: The input args unchanged.
    
    Raises:
        None
    
    Example:
        >>> _check_if_pandas_df(['a', 'b'])
        ['a', 'b']
    """
    return args

def get_column_names(args, return_str_list: bool=False) -> str|list[str]:
    """
    Convert iterable container and convert it into a single string joined by ', '.

    Used to create columns for an INSERT statement into a SQL database.
    Handles pandas DataFrames, dictionaries, lists, and tuples.

    Args:
        args (pd.DataFrame | dict | list | tuple): An iterable containing strings or 
            elements that can be converted to strings.
        return_str_list (bool, optional): Whether to return a list of strings instead 
            of a single string. Defaults to False.

    Returns:
        str|list[str]: A string in the format of 'arg_1, arg_2, ..., arg_n' or 
            a list of strings if return_str_list is True.
            
    Raises:
        ValueError: If unsupported type is provided after validation.

    Example:
        >>> args = ['arg_1', 'arg_2', 'arg_3']
        >>> get_column_names(args)
        'arg_1, arg_2, arg_3'
        >>> args = {'arg_1': 1, 'arg_2': 2, 'arg_3': 3}
        >>> get_column_names(args, return_str_list=True)
        ['arg_1', 'arg_2', 'arg_3']
    """
    # Check if the args are pd.DataFrame
    val_args = _check_if_pandas_df(args)

    # Convert val_args based on their type.
    if isinstance(val_args, dict):
        _args = val_args.keys()
    elif isinstance(val_args, (list,tuple)):
        _args = [str(arg) for arg in val_args] # Force convert each arg into a string if it isn't already.
    else:
        raise ValueError(f"Unsupported type after validation: '{type(val_args)}'")

    return ", ".join(_args) if not return_str_list else _args
