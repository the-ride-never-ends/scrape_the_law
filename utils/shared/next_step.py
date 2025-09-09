import re

from logger.logger import Logger

logger = Logger(logger_name=__name__,log_level=20)

def next_step(message: str, step: int=None, stop: bool=False):
    """
    Display a step message and optionally prompt for user continuation.
    
    This function logs step messages with special formatting and can pause execution
    to ask the user whether to continue. It parses step numbers from messages and
    handles user interruption of the program flow.
    
    Args:
        message (str): The step message to display. Can include "Step N" pattern.
        step (int, optional): Explicit step number. Extracted from message if None.
        stop (bool, optional): Whether to pause and ask user for continuation. 
            Defaults to False.
    
    Returns:
        None: This function performs side effects (logging, user input) but returns None.
    
    Raises:
        KeyboardInterrupt: Raised when user chooses not to continue execution.
    
    Example:
        >>> next_step("Step 1: Initialize data")
        # Logs: "Step 1: Initialize data" with formatting
        >>> next_step("Step 2: Process data", stop=True) 
        # Prompts: "Continue to Step 2? y/n: "
        # Raises KeyboardInterrupt if user enters anything other than "y"
    """

    step_pattern = re.compile(r'^Step \d+', flags=re.IGNORECASE)
    match = re.match(step_pattern, message)

    if stop:
        if match:
            step = int(re.search(r'\d+', match.group()).group())
        if match or step:
            current_step = step - 1
            result = input(f"Continue to Step {step}? y/n: ")
            if result != "y":
                raise KeyboardInterrupt(f"scrape_the_law program stopped at Step {current_step}.")
            else:
                logger.info(message, f=True)
                return
        else:
            result = input(f"Continue next step? y/n: ")
            if result != "y":
                raise KeyboardInterrupt(f"scrape_the_law program stopped at step.")
            else:
                logger.info(message, f=True)
                return
    else:
        logger.info(message, f=True)
        return
