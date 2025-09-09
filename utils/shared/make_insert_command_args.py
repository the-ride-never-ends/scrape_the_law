import re
from typing import Any

import pandas as pd

from .return_s_percent import return_s_percent


def make_insert_command_args(names: pd.DataFrame|dict|list, *args, command: str="", table_name: str="", **kwargs) -> dict[str, Any]:
    """
    Create arguments dictionary for parameterized SQL INSERT commands.
    
    Processes column names from various input types (DataFrame, dict, list) and creates
    a dictionary with table name, column names, and SQL placeholders. Validates that
    all placeholders in the command string have corresponding values.
    
    Args:
        names (pd.DataFrame|dict|list): Source of column names - DataFrame columns,
            dict keys, or list of strings.
        *args: Variable positional arguments, typically dictionaries to merge.
        command (str): SQL command string with placeholders like '{table}', '{column_names}'.
        table_name (str): Name of the database table for the INSERT operation.
        **kwargs: Additional keyword arguments to include in the result dictionary.
    
    Returns:
        dict[str, Any]: Dictionary containing 'table', 'column_names', 'values' and
            any additional arguments provided. Ready for use with string formatting.
    
    Raises:
        AssertionError: If command or table_name are missing or not strings.
        ValueError: If names is empty, unsupported type, or command has no placeholders.
        KeyError: If command placeholders don't match provided arguments.
    
    Example:
        >>> df = pd.DataFrame({'id': [1], 'name': ['John']})
        >>> cmd = "INSERT INTO {table} ({column_names}) VALUES ({values})"
        >>> args = make_insert_command_args(df, command=cmd, table_name="users")
        >>> args['table']
        'users'
        >>> args['column_names']
        'id, name'
        >>> args['values']
        '%s, %s'
    """

    assert command, f"command argument is missing"
    assert table_name, "table_string is missing"
    assert isinstance(command, str), f"command argument is not a string, but a {type(command)}"
    assert isinstance(table_name, str), f"table_name argument is not a string, but a {type(table_name)}"

    if isinstance(names, pd.DataFrame):
        _column_names = names.columns.tolist()
    elif isinstance(names, (dict, list,)):
        _column_names = list(names.keys()) if isinstance(names, dict) else names
    else:
        raise ValueError(f"Names is not a pd.DataFrame, list, or dict, but a {type(names)}")

    if not _column_names:
        raise ValueError("Column names list is empty")
    else:
        column_names = ", ".join(_column_names)

    args_dict = {
        "table": table_name,
        "column_names": column_names,
        "values": return_s_percent(column_names)
    }

    # Check if a dictionary variable was passed as a positional argument.
    if len(args) == 1 and isinstance(args[0], dict):
        args_dict.update(args[0])
    elif args and all(isinstance(arg, dict) for arg in args): 
        for arg in args:
            args_dict.update(arg)
    else:
        try:
            args_dict.update(kwargs)
        except:
            raise ValueError("No valid arguments provided. Expected either a dictionary as a positional argument or keyword arguments.")

    # Find all keys between curly braces in the command string
    command_keys = set(re.findall(r'\{(\w+)\}', command))

    if not command_keys:
        raise ValueError("Command string does not contain any placeholders")

    # Check if all keys in args are present in the command string
    missing_keys = command_keys - set(args_dict.keys())
    if missing_keys:
        raise KeyError(f"Keys {missing_keys} not found in command string")

    return args_dict
