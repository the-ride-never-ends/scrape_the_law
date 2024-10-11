import os 

from .make_csv_file_path_with_cwd import make_csv_file_path_with_cwd
from .save_to_csv import save_to_csv
from .load_from_csv import load_from_csv


from config import LEGAL_WEBSITE_DICT
from logger import Logger
logger = Logger(logger_name=__name__)


def merge_csv_files(filename: str) -> None:
    if not os.path.exists(make_csv_file_path_with_cwd(filename)):
        result_list = []
        for file in LEGAL_WEBSITE_DICT.keys():
            path = make_csv_file_path_with_cwd(file)
            logger.debug(f"path: {path}")
            if os.path.exists(path):
                logger.info(f"Got {file}")
                result_list.extend(load_from_csv(path))
        logger.debug(result_list)
        save_to_csv(result_list, make_csv_file_path_with_cwd(filename))
    else:
        logger.info(f"{filename}.csv already exists.")
    return