import os

from ....shared.load_from_csv import load_from_csv
from ....shared.save_to_csv import save_to_csv

from logger import Logger

logger = Logger(logger_name=__name__)

def save_urls_to_csv(source: str) -> None:
    csv_files = [
        file for file in os.listdir() if source in file and file.endswith(".csv")
    ]
    count = len(csv_files)
    logger.info(f"Found {len(csv_files)} {source} CSV files.")
    if count < 50:
        logger.warning(f"Expected 50 {source} CSV files, but found only {count}.")
        yes_or_no = input("Continue? y/n: ")
        if yes_or_no in ("n", "N"):
            logger.info("Stopping program...")
            raise KeyboardInterrupt("User chose to stop the program prematurely.")
    urls = []
    for file in csv_files:
        result = load_from_csv(file)
        urls.extend(result)
    logger.debug(f"{source}: {urls}")
    save_to_csv(urls, f"{source}_results.csv")
    return
