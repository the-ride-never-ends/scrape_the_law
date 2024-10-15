import csv

from logger import Logger

logger = Logger(logger_name=__name__)

def save_to_csv(data: list[dict], filename: str) -> None:
    if not data:
        logger.warning("No data to save.")
        return
    
    keys = data[0].keys()
    
    with open(filename, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    
    logger.info(f"Data saved to {filename}")
