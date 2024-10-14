
from logger import Logger
logger = Logger(logger_name=__name__)

from config import INPUT_FILENAME, VERBOSITY, START, OUTPUT_FOLDER, DELAY, WAIT_TIME, DATABASE_NAME, ROUTE, INTERNET_ARCHIVE_API_KEY
from database import MySqlDatabase

class ScrapeInternetArchive:

    def __init__(self):
        self.db: MySqlDatabase = None

    def scrape(self):
        pass


