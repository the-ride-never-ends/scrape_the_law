import asyncio
import os
import time


from selenium.webdriver.common.by import By
from selenium import webdriver


from utils.shared.sanitize_filename import sanitize_filename
from .load_from_csv import load_from_csv
from .save_to_csv import save_to_csv

from logger import Logger

logger = Logger(logger_name=__name__)


async def get_municode_urls(url: str, 
                            driver: webdriver.Chrome, 
                            wait_in_seconds: int, 
                            sleep_length: int=5) -> list[dict[str, str, str]]:
    """
    Get a list of places (towns/counties) by state from municode.com

    Args:
        url (str): URL to scrape
        driver (webdriver.Chrome): A Chromium webdriver

    Returns:
        data_out (pd.DataFrame): a dataframe of towns by state
    """
    url_filename = sanitize_filename(url)
    url_file_path = os.path.join(os.getcwd(), f"{url_filename}.csv")
    xpath_init = "//a[@class='index-link']" # "(//a[@class='index-link'])[1]/@href" #X_PATH_DICT['get_places_by_state']

    # assert that the driver is webdriver.Chrome
    assert isinstance(driver, webdriver.Chrome)

    if os.path.exists(url_file_path):
        logger.info(f"Got URL '{url}' already. Loading csv...")
        results = load_from_csv(url_file_path)
        return results
    else:
        driver.refresh()
        logger.info(f"Waiting {wait_in_seconds} seconds per Municode's robots.txt")
        time.sleep(wait_in_seconds)

    logger.info(f"Getting URL {url}...")
    driver.get(url)
    logger.info(f"URL ok. Searching for elements along x-path '{xpath_init}'")

    elements = driver.find_elements(by=By.XPATH, value=xpath_init)
    if len(elements) == 0:
        logger.warning(f"No elements found for URL {url}. Check the x-path. Returning...")
        return

    logger.info(f"Elements found. Waiting {sleep_length} seconds...")
    await asyncio.sleep(sleep_length)

    logger.info("Getting place URLs from outerHTML elements...")
    results = [
        {
        "url": url,
        "href": element.get_attribute('href'),
        "text": element.text.strip()
        } for element in elements
    ]
    logger.info(f"Got {len(results)} URLs.")

    save_to_csv(results, f"{url_filename}.csv")

    return results