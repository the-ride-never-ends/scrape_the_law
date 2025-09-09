
from logger.logger import Logger
logger = Logger(logger_name=__name__)

from config.config import INPUT_FILENAME, VERBOSITY, START, OUTPUT_FOLDER, DELAY, WAIT_TIME, DATABASE_NAME, ROUTE
from database.database import MySqlDatabase

class ScrapeInternetArchive:

    def __init__(self):
    """
      init   function.
    
    TODO: Add proper description.
    
    Args:
        None
    
    Returns:
        Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> __init__()
    """
        self.db: MySqlDatabase = None

    def scrape(self):
    """
    Scrape function.
    
    TODO: Add proper description.
    
    Args:
        None
    
    Returns:
        Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> scrape()
    """
        pass


