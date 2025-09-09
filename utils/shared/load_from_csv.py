import csv

from logger.logger import Logger

logger = Logger(logger_name=__name__)

def load_from_csv(filename: str) -> list[dict]:
    """
    Load data from a CSV file and return it as a list of dictionaries.

    Each row in the CSV is converted to a dictionary, with column names as keys.
    The function explicitly skips the header row after reading it for column names.

    Args:
        filename (str): The path to the CSV file to be loaded.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents a row
            from the CSV file. Returns an empty list if the file is not found or if
            there's an error reading the CSV.

    Raises:
        FileNotFoundError: If the specified file doesn't exist (handled gracefully).
        csv.Error: If there's an error reading the CSV file (handled gracefully).

    Example:
        >>> load_from_csv('data/sample.csv')
        [{'name': 'John', 'age': '30'}, {'name': 'Jane', 'age': '25'}]
        >>> load_from_csv('nonexistent.csv')
        []
    """
    try:
        logger.debug(f"filename: {filename}")
        with open(filename, 'r', newline='') as input_file:
            dict_reader = csv.DictReader(input_file)
            next(dict_reader)  # Skip the header row
            data = list(dict_reader)

        logger.info(f"Data loaded from {filename}")
        return data
    except FileNotFoundError:
        logger.error(f"File {filename} not found.")
        return []
    except csv.Error as e:
        logger.error(f"Error reading CSV file {filename}: {e}")
        return []
