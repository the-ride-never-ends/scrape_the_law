from datetime import datetime

def get_formatted_datetime():
    """
    Get the current date and time in "%Y-%m-%d %H:%M:%S" format.
    
    Returns the current date and time as a formatted string suitable for
    logging, database storage, or display purposes.

    Args:
        None
    
    Returns:
        str: Current datetime formatted as "YYYY-MM-DD HH:MM:SS".
    
    Raises:
        None
    
    Example:
        >>> datetime_str = get_formatted_datetime()
        >>> len(datetime_str)
        19
        >>> '-' in datetime_str and ':' in datetime_str
        True
        # Actual output varies by current time:
        # '2024-09-11 11:13:00'
    """

    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

