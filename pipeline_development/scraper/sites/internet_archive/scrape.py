
from logger.logger import Logger
logger = Logger(logger_name=__name__)

from config.config import INPUT_FILENAME, VERBOSITY, START, OUTPUT_FOLDER, DELAY, WAIT_TIME, DATABASE_NAME, ROUTE
from database.database import MySqlDatabase

class ScrapeInternetArchive:

    def __init__(self):
        self.db: MySqlDatabase = None

    def scrape(self):
        pass


