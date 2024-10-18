import time


from bs4 import BeautifulSoup
import requests
from selenium.common.exceptions import WebDriverException
from selenium import webdriver


from .get_municode_urls_from_state_landing_page import get_municode_urls_from_state_landing_page
from ....shared.get_urls_with_selenium import get_urls_with_selenium

from config import LEGAL_WEBSITE_DICT
from logger import Logger
logger = Logger(logger_name=__name__)


async def get_urls(urls: str, source: str=None, driver: webdriver.Chrome=None) -> list[dict[str,str,str]]:
    wait_in_seconds = LEGAL_WEBSITE_DICT[source]['wait_in_seconds']
    class_ = LEGAL_WEBSITE_DICT[source]['target_class']
    results = []

    if source == "municode":
        for url in urls:
            try:
                result = await get_municode_urls_from_state_landing_page(url, driver, wait_in_seconds)
                results.append(result)
            except WebDriverException as e:
                logger.error(f"Selenium error retrieving {url}: {e}")
    elif source == "general_code":
        for url in urls:
            try:
                result = await get_urls_with_selenium(url, driver, wait_in_seconds, source=source)
                results.append(result)
            except WebDriverException as e:
                logger.error(f"Selenium error retrieving {url}: {e}")
    else: # Default path using Beautiful Soup
        for url in urls:
            logger.info(f"Getting URL {url} from {source}...")
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    logger.info(f"URL {url} OK. Parsing content...")
                    # logger.debug(response.content)
                    # time.sleep(5)

                    # Parse the HTML content
                    soup = BeautifulSoup(response.content, 'html.parser')

                    # Find all elements within the class.
                    links = soup.find_all(class_=class_)
                    logger.info(f"Found {len(links)} links under class {class_}")
                    
                    # Extract href and text from each link
                    for link in links:
                        href = link.get('href')
                        text = link.text.strip()
                        results.append({
                            'url': url,
                            'href': href,
                            'text': text
                        })

                    time.sleep(wait_in_seconds)
                else:
                    logger.warning(f"Failed to retrieve {url}. Status code: {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"Error retrieving {url}: {e}")

    return results
