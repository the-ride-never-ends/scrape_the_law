import csv

from logger.logger import Logger

logger = Logger(logger_name=__name__)

def save_to_csv(data: list[dict] | list[str], filepath: str) -> None:
    """
    Save a list of dictionaries or strings to a CSV file.
    
    Handles both list of dictionaries (with headers) and list of strings (without headers).
    For dictionaries, uses the keys from the first dictionary as column headers.
    Logs warnings for empty data and success messages upon completion.
    
    Args:
        data (list[dict] | list[str]): Data to save - either list of dictionaries
            or list of strings.
        filepath (str): Path where the CSV file should be saved.
    
    Returns:
        None: This function performs file I/O side effects.
    
    Raises:
        IOError: If file cannot be written to the specified filepath.
        TypeError: If data format is neither list of dicts nor list of strings.
    
    Example:
        >>> data = [{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}]
        >>> save_to_csv(data, 'users.csv')
        # Creates CSV with headers: name,age
        >>> strings = ['line1', 'line2', 'line3']
        >>> save_to_csv(strings, 'output.csv')
        # Creates CSV with raw string data
    """
    if not data:
        logger.warning("No data to save.")
        return

    with open(filepath, 'w', newline='') as output_file:
        if isinstance(data[0], dict): # List of dictionaries route.
            keys = data[0].keys()
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        elif isinstance(data[0], str): # List of strings route
            csv_writer = csv.writer(output_file)
            csv_writer.writerows(data)
        else:
            logger.error("Invalid data format. Expected list of dictionaries or list of lists.")
            return

    logger.info(f"Data saved to {filepath}")
