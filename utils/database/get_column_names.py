

try:
    # NOTE Since this function might be used in code with or without the Pandas library,
    # we do this to make sure we can still use the function.
    import pandas as pd

    def _check_if_pandas_df(args: pd.DataFrame|tuple|dict|list[str]) -> list[str]:
        return args.columns.to_list()

except ImportError:

    def _check_if_pandas_df(args: tuple|dict|list[str]) -> tuple|dict|list[str]:
        return args

def get_column_names(args, return_str_list: bool=False) -> str|list[str]:
    """
    Conver interable container of some kind and convert it into a single string joined by ', '

    Used to create columns for an INSERT statement into a SQL database.

    Args:
        args (pd.DataFrame | dict | list | tuple): An iterable containing strings or elements that can be converted to strings.
        return_str_list (bool, Optional): Boolean for whether or not to return a list of strings instead of a single string. Defaults to False.

    Returns:
        A string in the format of 'arg_1, arg_2, ..., arg_n' or a list of strings.

    Examples:
        >>> args = ['arg_1', 'arg_2', 'arg_3']
        >>> get_column_names(args)
        'arg_1, arg_2, arg_3'
        >>> args = {'arg_1': 1, 'arg_2': 2, 'arg_3': 3}
        >>> get_colunm_names(args, return_str_list=True)
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
