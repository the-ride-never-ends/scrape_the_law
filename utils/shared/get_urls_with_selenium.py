import abc
import os
import time
from typing import NamedTuple


import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    StaleElementReferenceException, 
    WebDriverException,
    NoSuchElementException, 
    TimeoutException,
    InvalidArgumentException
)


from utils.shared.sanitize_filename import sanitize_filename
from .load_from_csv import load_from_csv
from .save_to_csv import save_to_csv
from utils.shared.decorators.try_except import try_except
from utils.shared.decorators.adjust_wait_time_for_execution import adjust_wait_time_for_execution
from utils.shared.decorators.get_exec_time import get_exec_time

from config.config import LEGAL_WEBSITE_DICT, OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__)

output_folder = os.path.join(OUTPUT_FOLDER, "get_sidebar_urls_from_municode")
if not os.path.exists(output_folder):
    print(f"Creating output folder: {output_folder}")
    os.mkdir(output_folder)


from scraper.child_classes.selenium.SeleniumScraper import SeleniumScraper



from collections import deque

class GetMunicodeSidebarElements(SeleniumScraper):
    """
    Get the sidebar elements from a Municode URL page.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xpath_dict = {
            "current_version": '//*[@id="codebankToggle"]/button/text()',
            "version_button": '//*[@id="codebankToggle"]/button/',
            "version_text_paths": '//mcc-codebank//ul/li//button/text()',
            'toc': "//input[starts-with(@id, 'genToc_')]" # NOTE toc = Table of Contents
        }
        self.queue = deque()
        self.output_folder = output_folder



    def _get_current_code_version(self, url: str) -> str:
        """
        Get the date for the current version of the municipal code.
        """
        result: WebElement = self.find_elements_by_xpath(url, xpath=self.xpath_dict['current_version'])
        logger.debug(f'code_verion: {result.text}')
        return result.text.strip()


    def _get_all_code_versions(self, url: str) -> list[str]:
        """
        Get the dates for current and past versions of the municipal code.
        NOTE: You need to click on each individual button to get the link itself.
        """
        # Press the button that shows the code archives pop-up
        self.press_buttons(url, xpath=self.xpath_dict['version_button'])

        # Get all the dates in the pop-up.
        buttons = self.wait_for_and_then_return_elements(
            self.xpath_dict['version_text_paths'], wait_time=10, poll_frequency=0.5
        )
        version_list = [
            button.text.strip() for button in buttons
        ]
        logger.debug(f'version_list\n{version_list}',f=True)
        return version_list


    def _scrape_toc(self, base_url: str, wait_time: int) -> list[dict]:
        """
        Scrape a Municode URL's Table of Contents.
        """
        #Initialize the queue
        initial_xpath = "//li[@ng-repeat='node in toc.topLevelNodes track by node.Id']"
        self.queue.append((self.driver.current_url, initial_xpath))
        all_data = []

        element.get_attribute('innerHTML')
        while self.queue:
            # Get the current url and its xpath from the queue
            current_url, xpath = self.queue.popleft()

            # Push the sidebar button.
            # NOTE This does not appear when going to the site in a regular Chrome browser.

            # Get all the elements currently in the sidebar.
            elements = WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_all_elements_located((By.XPATH, xpath))
            )

            for element in elements:
                element: WebElement
                # Extract the href and text from them.
                data = self.extract_data(element)
                all_data.append(data)

                # Find all the expandable buttons in the toc.
                expand_button = element.find_element(By.CSS_SELECTOR, 'button.toc-item-expand')

                if expand_button and not expand_button.get_attribute('aria-expanded') == 'true':
                    expand_button.click()  # Click on the node
                    self.wait_for_aria_expanded(element, state='true', timeout=wait_time) # Wait for it to expand.
                    # Get the new x-paths and add them to the queue
                    child_xpath = f"{xpath}/ul/li[@ng-repeat='node in node.Children track by node.Id']" 
                    self.queue.append((current_url, child_xpath))

        return all_data


    def extract_data(self, node: WebElement) -> dict:
        """
        Extract and return the heading and href data from a toc node
        """
        select = node.find_element(By.CSS_SELECTOR, 'a')
        return {'heading': select.text, 'href': select.get_attribute('href')}


    def _skip_if_we_have_url_already(self, url: str) -> list[dict]|None:
        """
        Check if we already have a CSV file of the input URL. 
        If we do, load it as a list of dictionaries and return it. Else, return None
        """
        url_filename = sanitize_filename(url)
        url_file_path = os.path.join(OUTPUT_FOLDER, f"{url_filename}.csv")
        if os.path.exists(url_file_path):
            logger.info(f"Got URL '{url}' already. Loading csv...")
            output_dict = load_from_csv(url_file_path)
            return output_dict
        else:
            return None


    # Decorator to wait per Municode's robots.txt
    # NOTE Since code URLs are processed successively, we can subtract off the time it took to get all the pages elements
    # from the wait time specified in robots.txt. This should speed things up (?).
    @adjust_wait_time_for_execution(wait_in_seconds=LEGAL_WEBSITE_DICT["municode"]["wait_in_seconds"])
    def get_municode_sidebar_elements(self, 
                                      i: int,
                                      row: NamedTuple,
                                      len_df: int,
                                      ) -> dict:
        """
        Extract the code versions and table of contents from a city's Municode page.
        NOTE This function orchestrates all the methods of this class, similar to main.py

        Example Input:
            row
            Pandas(Index=0, 
                    url=https://library.municode.com/az/cottonwood, 
                    gnis: 12345, 
                    place_name: Town of Cottonwood,
                    url_hash=ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5...)

        Example Output:
            output_dict = {
                'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
                'input_url': "https://library.municode.com/az/cottonwood",
                'gnis': 123456789,
                'current_code_version': 'July 26th, 2024',
                'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
                'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
            }
        """
        logger.info(f"Processing URL {i} of {len_df}")
        # Check to make sure the URL is a municode one, then initialize the dictionary.
        assert "municode" in row.url, f"URL '{row.url}' is not for municode."
        input_url = row.url
        wait_time = 10

        # Skip the webpage if we already got it.
        output_dict = self._skip_if_we_have_url_already(input_url)
        if output_dict:
            return output_dict

        output_dict = {'url_hash': row.url_hash, 'input_url': input_url, 'gnis': row.gnis}

        # Open the webpage.
        self.make_page(input_url)

        toc_button = """
        //button[contains(@class, 'btn-success') and contains(., 'View what')]
        """
        browser_toc_button = "btn btn-raised btn-primary"

        # Wait for the webpage to fully load, based on the button element.
        self.wait_to_fully_load(implicit_wait=15, class_name=browser_toc_button)
        time.sleep(15)

        self.driver.save_screenshot(os.path.join(output_folder, f"{row.gnis}_screenshot.png"))

        # Get the html of the opening page.
        html_content = self.driver.page_source

        # Write the HTML content to a file
        _path = os.path.join(output_folder, f"{row.gnis}_opening_webpage.html")
        with open(_path, "w", encoding="utf-8") as file:
            file.write(html_content)
            print(f"HTML content from {input_url} has been saved to webpage.html")
        raise # TODO Remove this line after debug.

        # Get the URLs from the table of contents.
        # NOTE The tables are recursive, so this won't get all the buried URLs. 
        toc_urls = self._scrape_toc(input_url, wait_time=wait_time)
        output_dict["table_of_contents_urls"] = toc_urls

        # Get the date the code was last updated.
        output_dict['current_code_version'] = self._get_current_code_version(input_url)

        # Get all the previous dates the code was updated.
        # NOTE This does not grab their links, just their text.
        all_code_versions: list[str] = self._get_all_code_versions(input_url)
        output_dict['all_code_versions'] = all_code_versions

        logger.info(f"Found all elements from Municode URL for gnis '{row.gnis}'")
        # Export output_dict as a CSV file for safe-keeping.
        url_file_path = os.path.join(output_folder, f"{sanitize_filename(input_url)}.csv")
        save_to_csv(output_dict, url_file_path)

        return output_dict

from utils.shared.make_sha256_hash import make_sha256_hash

def make_urls_df(output_list: list[dict]) -> pd.DataFrame:
    """
    Make urls_df

    Example Input:
    >>> output_list = [{
    >>>     'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
    >>>     'input_url': "https://library.municode.com/az/cottonwood",
    >>>     'gnis': 123456789,
    >>>     'current_code_version': 'July 26th, 2024',
    >>>     'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
    >>>     'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
    >>> },...]

    Example Output:
        >>> urls_df.head()
        >>> url_hash    query_hash              gnis    url
        >>> 3beb75cb    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH82TA_ARTIILERETA
        >>> 4648a64b    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH26BU_ARTIINGE_S26-2IMUNTABU
        >>> 58cd5049    not_found_from_query    156909  https://library.municode.com/.../PTIICO_CH98ZO_ARTIVSURE_DIV2OREPALORE
        >>> 76205dbb    not_found_from_query    156909  https://ecode360.com/WE1870/document/224899757.pdf
        >>> 30935d36    not_found_from_query    156909  https://ecode360.com/NE0395/document/647960636.pdf
        >>> 792b4192    not_found_from_query    254139  https://ecode360.com/LO1625/document/430360980.pdf
        >>> 792b4192    not_found_from_query    254139  https://ecode360.com/LO1625/document/430360980.pdf
    """
    # Turn the list of dicts into a dataframe.
    urls_df = pd.DataFrame.from_records(output_list)

    # Make url hashes for each url
    urls_df['url_hash'] = urls_df.apply(lambda row: make_sha256_hash(row['gnis'], row['url']))

    # Rename toc urls to match the format of the table 'urls' in the MySQL database.
    urls_df.rename(columns={"table_of_contents_urls": "url"})

    # Add the dummy query_hash column.
    urls_df['query_hash'] = "not_found_from_query"

    # Drop the code version columns.
    urls_df.drop(['current_code_version','all_code_versions'], axis=1, inplace=True)

    return urls_df


def save_code_versions_to_csv(output_list: list[dict]) -> None:
    """
    Example Input:
    >>> output_list = [{
    >>>     'url_hash': ed22a03bd810467b0fe30f1306a2aaa9c1d047d9799be5,
    >>>     'input_url': "https://library.municode.com/az/cottonwood",
    >>>     'gnis': 123456789,
    >>>     'current_code_version': 'July 26th, 2024',
    >>>     'all_code_versions': ['July 26th, 2024', June 4th, 2023'],
    >>>     'table_of_contents_urls': ['www.municode_example69.com',''www.municode_example420.com'],
    >>> },...]
    """

    # Turn the list of dicts into a dataframe.
    code_versions_df = pd.DataFrame.from_records(output_list)

    # Drop the urls columns.
    code_versions_df.drop(['url_hash','table_of_contents_urls'], axis=1, inplace=True)

    # Save the dataframe to the output folder.
    output_file = os.path.join(output_folder, sanitize_filename(output_list[0]['input_url']))
    save_to_csv(code_versions_df, output_file)

    return

# https://library.municode.com/

def get_sidebar_urls_from_municode_with_selenium(df: pd.DataFrame, wait_in_seconds: int) -> pd.DataFrame:
    """
    Get href and text of sidebar elements in a Municode city code URL.
    """

    # Initialize webdriver.
    logger.info("Initializing webdriver...")
    options = webdriver.ChromeOptions() # Define options for the webdriver.
    #options.add_argument("--headless")  # Run in headless mode (optional). Off for the moment.
    driver = webdriver.Chrome(options=options)
    logger.info("Webdriver initialized.")

    # Get the sidebar URLs and text for each Municode URL
    # NOTE Adding the 'if row else None' is like adding 'continue' to a regular for-loop.
    len_df = len(df)
    with GetMunicodeSidebarElements(wait_in_seconds=wait_in_seconds, driver=driver) as municode:

        logger.info(f"Starting get_municode_sidebar_elements loop. Processing {len(df)} URLs...")
        # Get the side bar elements for each Municode URL.
        list_of_lists_of_dicts: list[dict] = [ 
            municode.get_municode_sidebar_elements(i, row, len_df) if row else None for i, row in enumerate(df.itertuples(), start=1)
        ]

    logger.info("get_municode_sidebar_elements loop complete. Flattening...")
    # Flatten the list of lists of dictionaries into just a list of dictionaries.
    output_list = [item for sublist in list_of_lists_of_dicts for item in sublist]
    
    logger.info("get_sidebar_urls_from_municode_with_selenium function complete. Making dataframes and saving...")

    urls_df = make_urls_df(output_list)
    save_code_versions_to_csv(output_list)

    return urls_df



# async def _get_urls_with_selenium(url: str, 
#                                 driver: webdriver.Chrome,
#                                 wait_in_seconds: int, 
#                                 sleep_length: int=5,
#                                 source: str=None,
#                                 class_: str=None,
#                                 allow_redirects: bool=True,) -> list[dict[str, str]]:
#     """
#     Get a list of places (towns/counties)

#     Args:
#         url (str): URL to scrape
#         driver (webdriver.Chrome): A Chromium webdriver

#     Returns:
#         data_out (pd.DataFrame): a dataframe of towns by state
#     """
#     if not source:
#         raise ValueError("source argument not specified.")

#     url_filename = sanitize_filename(url)
#     url_file_path = os.path.join(os.getcwd(), f"{url_filename}.csv")
#     class_ = class_ or LEGAL_WEBSITE_DICT[source]['target_class']
#     xpath_init = f"//a[@class='{class_}']"

#     # assert that the driver is webdriver.Chrome
#     assert isinstance(driver, webdriver.Chrome)

#     if os.path.exists(url_file_path):
#         logger.info(f"Got URL '{url}' already. Loading csv...")
#         results = load_from_csv(url_file_path)
#         return results
#     else:
#         driver.refresh()
#         logger.info(f"Waiting {wait_in_seconds} seconds per {source}'s robots.txt")
#         time.sleep(wait_in_seconds)

#     logger.info(f"Getting URL {url}...")
#     driver.get(url)
#     logger.info(f"URL ok. Searching for elements along x-path '{xpath_init}'")

#     elements = driver.find_elements(by=By.XPATH, value=xpath_init)
#     if len(elements) == 0:
#         logger.warning(f"No elements found for URL {url}. Check the x-path. Returning...")
#         return

#     # logger.debug(f"elements: {elements}")

#     logger.info(f"Elements found. Waiting {sleep_length} seconds...")
#     await asyncio.sleep(sleep_length)

#     logger.info("Getting place URLs from outerHTML elements...")
#     results = [
#         {
#         "url": url,
#         "href": element.get_attribute('href'),
#         "text": element.text.strip()
#         } for element in elements
#     ]
#     logger.info(f"Got {len(results)} URLs.")

#     save_to_csv(results, f"{url_filename}.csv")

#     return results