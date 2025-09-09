
from .get_num_placeholders import get_num_placeholders
from .get_column_names import get_column_names

def get_insert_into_values(args: dict|list[str]) -> tuple[str,str]:
    """
    Get column names and SQL placeholders for INSERT statements.
    
    Convenience function that combines get_column_names and get_num_placeholders
    to provide both the column list and placeholder string needed for SQL INSERT
    operations.
    
    Args:
        args (dict|list[str]): Dictionary with keys as column names or list of column names.
    
    Returns:
        tuple[str,str]: Tuple containing (column_names_string, placeholders_string).
            First element is comma-separated column names, second is comma-separated "%s" placeholders.
    
    Raises:
        ValueError: If args is of unsupported type or empty.
        TypeError: If args doesn't support len() function.
    
    Example:
        >>> columns, placeholders = get_insert_into_values(['id', 'name', 'email'])
        >>> print(columns)
        'id, name, email'
        >>> print(placeholders)
        '%s, %s, %s'
        >>> columns, placeholders = get_insert_into_values({'id': 1, 'name': 'John'})
        >>> print(columns)
        'id, name'
    """
    return get_column_names(args), get_num_placeholders(args)
