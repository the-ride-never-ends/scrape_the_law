import csv

from logger.logger import Logger

logger = Logger(logger_name=__name__)

def save_to_csv(data: list[dict] | list[str], filepath: str) -> None:
    """
    Save a list of dictionaries or a list of strings to a CSV file.
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
