

def get_num_placeholders(args: dict|list) -> str:
    """
    Generate a comma-separated string of SQL placeholders (%s) based on input length.
    
    Creates a string of "%s" placeholders separated by commas, matching the length
    of the input. Used for generating SQL parameter placeholders dynamically.
    
    Args:
        args (dict|list): An iterable whose length determines the number of placeholders.
    
    Returns:
        str: Comma-separated string of "%s" placeholders.
    
    Raises:
        TypeError: If args doesn't support len() function.
    
    Example:
        >>> get_num_placeholders([1, 2, 3])
        '%s, %s, %s'
        >>> get_num_placeholders({'a': 1, 'b': 2})
        '%s, %s'
        >>> get_num_placeholders([])
        ''
    """
    placeholders = ", ".join(["%s"] * len(args))
    return placeholders

