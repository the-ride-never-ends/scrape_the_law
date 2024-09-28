from datetime import datetime
import logging
import os
import uuid

import yaml

from utils.logger.delete_empty_log_files import delete_empty_log_files

def make_id():
    return str(uuid.uuid4())

# Define general folder for log files
base_path = os.path.dirname(os.path.realpath(__file__))
debug_log_folder = os.path.join(base_path, "debug_logs")

# Import DEBUG config
# We do a separate yaml import to avoid circular imports with the config file.
script_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_dir, './config.yaml')
try:
    delete_empty_log_files(debug_log_folder)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    DEFAULT_LOG_LEVEL = config['SYSTEM']['DEFAULT_LOG_LEVEL']
    FORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM: bool = config['SYSTEM']['FORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM']
    print(f"DEFAULT_LOG_LEVEL set to '{DEFAULT_LOG_LEVEL}'\nFORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM set to {FORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM}")
except Exception as e:
    # Automatically run the entire program in debug mode if we lack configs.
    DEFAULT_LOG_LEVEL = logging.DEBUG
    FORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM = True
    print(f"Could not get debug level from config.yaml. Default LOG_LEVEL set to '{DEFAULT_LOG_LEVEL}'\nDefault FORCE_DEFAULT_LOG_LEVEL set to '{FORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM}'")

# Get the program's name
PROGRAM_NAME = os.path.dirname(__file__) 

# NOTE
# CRITICAL = 50
# FATAL = CRITICAL
# ERROR = 40
# WARNING = 30
# WARN = WARNING
# INFO = 20
# DEBUG = 10
# NOTSET = 0

class Logger:
    """
    Create a logger. Supports dynamic log folder generation and routing.\n
    Has a specialized prompt logger that can be called dynamically within an LLM engine.

    Parameters:
        logger_name: (str) Name for the logger. Defaults to the program's name i.e. the top-level directory's name.
        prompt_name: (str) Name of a prompt. Used by the prompt logger. Defaults to "prompt_log".
        batch_id: (str) The logger's batch id. Used by the prompt logger. Defaults to random UUID4 string.
        current_time: (datetime) The time a logger is initialized. Defaults to now() in "%Y-%m-%d_%H-%M-%S" format.
        log_level (int): The logging level. Defaults to DEFAULT_LOG_LEVEL from config or logging.DEBUG if config is unavailable.

    Attributes:
        logger_name (str): The name of the logger.
        prompt_name (str): The name of the prompt (for prompt logging).
        batch_id (str): The batch ID for the logger.
        current_time (str): The initialization time of the logger.
        logger_folder (str): The folder where log files are stored.
        log_level (int): The current logging level.
        logger (logging.Logger): The underlying Python logger object.
        filename (str): The name of the log file.
        filepath (str): The full path to the log file.

    Methods:
        info(): Log a message with severity 'INFO'.
        debug(): Log a message with severity 'DEBUG'.
        warning(): Log a message with severity 'WARNING'.
        error(): Log a message with severity 'ERROR'.
        critical(): Log a message with severity 'CRITICAL'.
        exception(): Log a message with severity 'ERROR', as well as include exception information.

    Example:
        >>> from logger import Logger
        >>> filename = __name__ # example.py
        >>> logger = Logger(logger_name=filename)
        >>> logger.info("Hello world!") # line 4
        '2024-09-18 18:38:44,185 - example_logger - INFO - example.py: 4 - Hello world!'

    Note:
        The class uses the following log levels:
        CRITICAL = 50, ERROR = 40, WARNING = 30, INFO = 20, DEBUG = 10, NOTSET = 0
        FATAL is an alias for CRITICAL, and WARN is an alias for WARNING.
    """

    def __init__(self,
                 logger_name: str=PROGRAM_NAME,
                 prompt_name: str="prompt_log",
                 batch_id: str=make_id(),
                 current_time: datetime=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                 log_level=DEFAULT_LOG_LEVEL
                ):
        self.logger_name = logger_name
        self.prompt_name = prompt_name
        self.batch_id = batch_id
        self.current_time = current_time
        self.logger_folder = debug_log_folder
        self.log_level = log_level if not FORCE_DEFAULT_LOG_LEVEL_FOR_WHOLE_PROGRAM else DEFAULT_LOG_LEVEL
        self.stacklevel = None
        self.logger = None
        self.filename = None
        self.filepath = None

        # Create the specified log folder if it doesn't exist.
        # This assures that we always have a valid path for the log file.
        self.logger_folder = os.path.join(self.logger_folder, self.logger_name)
        if not os.path.exists(self.logger_folder):
            os.makedirs(self.logger_folder)

        # Determine properties of the logger based on its name.
        match logger_name:
            case logger_name if logger_name == PROGRAM_NAME: # If logger_name is the program's name
                self.logger = logging.getLogger(f"{PROGRAM_NAME}_logger")
                filename = f"{PROGRAM_NAME}_debug_log_{self.current_time}.log"
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s: %(lineno)d - %(message)s')
                self.stacklevel = 2 # We make stacklevel=2 as otherwise it'll give the filename and line numbers from the logger class itself.

            case "prompt":
                self.logger =  logging.getLogger(f"prompt_logger_for_{self.prompt_name}_batch_id_{self.batch_id}")
                filename =  f"{self.prompt_name}_{self.batch_id}_{self.current_time}.log"
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(lineno)d - %(message)s')
                self.stacklevel = 1 # Force the stack to log the generation class' name

            case _: # All other specialized loggers.
                self.logger = logging.getLogger(f"{self.logger_name}_logger")
                filename = f"{self.logger_name}_debug_log_{self.current_time}.log"
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s: %(lineno)d - %(message)s')
                self.stacklevel = 2

        # Create the logger itself.
        self.logger.setLevel(self.log_level)
        self.logger.propagate = False # Prevent logs from being handled by parent loggers

        if not self.logger.handlers:
            # Create handlers (file and console)
            self.filepath = os.path.join(self.logger_folder, filename)
            file_handler = logging.FileHandler(self.filepath)
            console_handler = logging.StreamHandler()

            # Set level for handlers
            file_handler.setLevel(logging.DEBUG)
            console_handler.setLevel(logging.DEBUG)

            # Create formatters and add it to handlers
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers to the logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def info(self, message):
        self.logger.info(message, stacklevel=self.stacklevel) 

    def debug(self, message):
        self.logger.debug(message, stacklevel=self.stacklevel)
    
    def warning(self, message):
        self.logger.warning(message, stacklevel=self.stacklevel)

    def error(self, message):
        self.logger.error(message, stacklevel=self.stacklevel)

    def critical(self, message):
        self.logger.critical(message, stacklevel=self.stacklevel)

    def exception(self, message):
        self.logger.exception(message, stacklevel=self.stacklevel)

###############################

# Create singletons of the loggers.
logger = Logger()
