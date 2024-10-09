from datetime import datetime

def convert_integer_to_datetime_str(integer: int) -> str:
    """
    Example:
    >>> integer_input = 20240802121308
    >>> result = convert_integer_to_datetime_str(integer_input)
    '2024-08-02 12:13:08'
    """

    # Convert the integer to a string
    date_string = str(integer)
    assert len(date_string) == 14, f"len(date_string) is not 14, but '{len(date_string)}', so it cannot be converted to YYYY-MM-DD hh:mm:ss format"
    
    # Parse the string into a datetime object
    dt = datetime.strptime(date_string, "%Y%m%d%H%M%S")
    
    # Format the datetime object into the desired string format
    formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    
    return formatted_date

