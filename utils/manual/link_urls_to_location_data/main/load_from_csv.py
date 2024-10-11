import csv

from logger import Logger

logger = Logger(logger_name=__name__)


def load_from_csv(filename: str) -> list[dict]:
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
