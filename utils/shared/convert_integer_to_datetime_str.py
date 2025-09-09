from datetime import datetime

def convert_integer_to_datetime_str(integer: int) -> str:
    """
    Convert a 14-digit integer timestamp to datetime string format.
    
    Takes an integer in YYYYMMDDHHMMSS format and converts it to a 
    human-readable datetime string in "YYYY-MM-DD HH:MM:SS" format.
    
    Args:
        integer (int): 14-digit integer in YYYYMMDDHHMMSS format.
    
    Returns:
        str: Formatted datetime string in "YYYY-MM-DD HH:MM:SS" format.
    
    Raises:
        AssertionError: If the integer doesn't have exactly 14 digits.
        ValueError: If the integer cannot be parsed as a valid datetime.
    
    Example:
        >>> convert_integer_to_datetime_str(20240802121308)
        '2024-08-02 12:13:08'
        >>> convert_integer_to_datetime_str(20231225120000)
        '2023-12-25 12:00:00'
    """

    # Convert the integer to a string
    date_string = str(integer)
    assert len(date_string) == 14, f"len(date_string) is not 14, but '{len(date_string)}', so it cannot be converted to YYYY-MM-DD hh:mm:ss format"
    
    # Parse the string into a datetime object
    dt = datetime.strptime(date_string, "%Y%m%d%H%M%S")
    
    # Format the datetime object into the desired string format
    formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    
    return formatted_date

