
import os

from utils.config.get_config import get_config as config
from logger import Logger
logger = Logger(logger_name=__name__)


# Define hard-coded constants
INPUT_FOLDER = script_dir = os.path.dirname(os.path.realpath(__file__))
YEAR_IN_DAYS: int = 365
DEBUG_FILEPATH: str = os.path.join(script_dir, "debug_logs")

# Dictionary of tax types and related terms
TAX_TERMS: dict[str, list[str]] = {
    "sales tax": [
        "tax", "indirect tax", "consumption tax", "local option sales tax", "municipal sales tax", "county sales tax", "city sales tax", 
        "combined sales tax", "transaction privilege tax", "general excise tax", "gross receipts tax", "retail sales tax"
    ],
    "income tax": ["tax", "direct tax", "progressive tax", "personal income tax", "corporate income tax"],
    "property tax": ["tax", "ad valorem tax", "real estate tax", "millage tax"],
    "payroll tax": ["tax", "employment tax", "social security tax", "Medicare tax", "FICA tax"],
    "consumption tax": ["tax", "sales tax", "value-added tax", "VAT", "goods and services tax", "GST"],
    "tariff": ["tax", "import duty", "customs duty", "excise tax"],
    "capitation": ["tax", "poll tax", "head tax", "per capita tax"],
}

# Get YAML config variables
try:
    # SYSTEM
    path = "SYSTEM"
    ROUTE: str = config(path, 'ROUTE') or "NA"
    CONCURRENCY_LIMIT: int = config(path, 'CONCURRENCY_LIMIT') or 2
    FILENAME_PREFIX: str = config(path, 'FILENAME_PREFIX') or "scrape_all_law"
    OUTPUT_FOLDER: str = config(path, 'OUTPUT_FOLDER') or script_dir
    WAIT_TIME: int = config(path, 'WAIT_TIME') or 0
    DATAPOINT: str = config(path, 'DATAPOINT') or "sales tax"

    # FILENAMES
    path = "FILENAMES"
    INPUT_FILENAME: str = config(path, 'INPUT_FILENAME') or "input.csv"

    # WAYBACKUP COMMAND OPTIONS - REQUIRED
    path = "ARCHIVE.WAYBACKUP.REQUIRED"
    URL: str = config(path, 'URL') or "http://example.com"
    CURRENT: bool = config(path, 'CURRENT') or False
    FULL: bool = config(path, 'FULL') or False
    SAVE: bool = config(path, 'SAVE') or False

    # Optional and behavior manipulation options
    path = "ARCHIVE.WAYBACKUP.OPTIONAL"
    LIST: bool = config(path, 'LIST') or False
    EXPLICIT: bool = config(path, 'EXPLICIT') or False
    OUTPUT: str = config(path, 'OUTPUT') or ""
    RANGE: str = config(path, 'RANGE') or ""
    START: str = config(path, 'START') or ""
    END: str = config(path, 'END') or ""
    FILETYPE: str = config(path, 'FILETYPE') or ""

    path = "ARCHIVE.WAYBACKUP.BEHAVIOR_MANIPULATION"
    CSV: bool = config(path, 'CSV') or False
    SKIP: bool = config(path, 'SKIP') or False
    NO_REDIRECT: bool = config(path, 'NO_REDIRECT') or False
    VERBOSITY: str = config(path, 'VERBOSITY') or "trace"
    LOG: bool = config(path, 'LOG') or False
    RETRY: int = config(path, 'RETRY') or 3
    WORKERS: int = config(path, 'WORKERS') or 1
    DELAY: int = config(path, 'DELAY') or 0
    LIMIT: int = config(path, 'LIMIT') or 0

    # CDX and auto configuration
    path = "ARCHIVE.WAYBACKUP.CDX"
    CDX_BACKUP: str = config(path, 'CDX_BACKUP') or ""
    CDX_INJECT: str = config(path, 'CDX_INJECT') or ""
    AUTO: bool = config(path, 'AUTO') or False

    # CLEAN DOCS
    path = "CLEAN_DOCS.JINJA"
    JINJA_URL: str = config(path, 'JINJA_URL') or ""
    JINJA_API_KEY: str = config(path, 'JINJA_API_KEY') or ""

    # INTERNET ARCHIVE
    path = "INTERNET_ARCHIVE"
    INTERNET_ARCHIVE_URL: str = config(path, 'INTERNET_ARCHIVE_URL') or "https://web.archive.org/"
    INTERNET_ARCHIVE_SAVE_URL: str = config(path, 'INTERNET_ARCHIVE_SAVE_URL') or "https://web.archive.org/save/"
    S3_ACCESS_KEY: str = config(path, "S3_ACCESS_KEY") or ""
    S3_SECRET_KEY: str = config(path, "S3_SECRET_KEY") or ""

    # SEARCH
    path = "SEARCH"
    SEARCH_ENGINE: str = config(path, 'SEARCH_ENGINE') or "google"
    GOOGLE_CONCURRENCY_LIMIT: int = config(path, 'GOOGLE_CONCURRENCY_LIMIT') or 2
    NUM_RESULTS: int = config(path, 'NUM_RESULTS') or 10

    path = "SEARCH.SITE_URLS"
    MUNICODE_URL: str = config(path, 'MUNICODE_URL') or "https://library.municode.com/"
    AMERICAN_LEGAL_URL: str = config(path, 'AMERICAN_LEGAL_URL') or "https://codelibrary.amlegal.com/codes/"
    GENERAL_CODE_URL: str = config(path, 'GENERAL_CODE_URL') or "https://ecode360.com/"
    CODE_PUBLISHING_CO_URL: str = config(path, 'CODE_PUBLISHING_CO_URL') or "https://www.codepublishing.com/"

    # MYSQL OPTIONS
    path = "MYSQL"
    HOST: str = config(path, 'HOST') or ""
    USER: str = config(path, 'USER') or ""
    PORT: int = config(path, 'PORT') or 3306
    PASSWORD: str = config(path, 'PASSWORD') or ""
    DATABASE_NAME: str = config(path, 'DATABASE_NAME') or ""
    MYSQL_SCRIPT_FILE_PATH: str = config(path, 'MYSQL_SCRIPT_FILE_PATH') or ""
    INSERT_BATCH_SIZE: int = config(path, 'INSERT_BATCH_SIZE') or 1000
    RAND_SEED: int = config(path, 'RAND_SEED') or 420

except KeyError as e:
    logger.exception(f"Missing configuration item: {e}")
    raise KeyError(f"Missing configuration item: {e}")

except Exception as e:
    logger.exception(f"Could not load configs: {e}")
    raise e

