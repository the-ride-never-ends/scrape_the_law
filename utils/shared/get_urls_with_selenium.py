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
from ..manual.link_urls_to_location_data.main.load_from_csv import load_from_csv
from .save_to_csv import save_to_csv
from utils.shared.decorators.try_except import try_except
from utils.shared.decorators.adjust_wait_time_for_execution import adjust_wait_time_for_execution

from config import LEGAL_WEBSITE_DICT
from logger import Logger
logger = Logger(logger_name=__name__)


class GetElementsWithPlaywright:
    def __init__(self, driver: webdriver.Chrome=None, wait_in_seconds: int=1, ):
        self.driver = driver
        self.wait_in_seconds = wait_in_seconds
        raise NotImplementedError("GetElementsWithPlaywright not implemented at the moment. Sorry!")


class GetElementsWithSelenium:

    def __init__(self, driver: webdriver.Chrome=None, wait_in_seconds: int=1, ):
        self.driver = driver
        self.wait_in_seconds = wait_in_seconds
        self.page = None

        if not self.driver:
            logger.error("Chrome webdriver not passed to Selenium.")
            raise ValueError("Chrome webdriver not passed to Selenium.")

    @try_except(exception=[WebDriverException])
    def close_webpage(self):
        return self.driver.close()

    @try_except(exception=[WebDriverException])
    def _quit_driver(self):
        return self.driver.quit()

    def __enter__(self):
        return self

    @classmethod
    def enter(cls, wait_in_seconds):
        """
        Factory method to start Selenium
        """
        instance = cls(wait_in_seconds)
        return instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit()
        return False

    def exit(self):
        """
        Close the webpage and webdriver.
        """
        if self.page:
            self.close_webpage()
        self._quit_driver()
        return

    @try_except
    def refresh_page(self) -> None:
        """
        Refresh the web page currently in the driver.
        """
        self.driver.refresh()

    @try_except(exception=[WebDriverException, InvalidArgumentException, TimeoutException])
    def make_page(self, url: str) -> None:
        """
        Navigate to the specified URL using the given Chrome webdriver.

        Args:
            url (str): The URL to navigate to.
        Raises:
            WebDriverException: If there's an issue with the WebDriver while navigating.
            TimeoutException: If the page load takes too long.
            InvalidArgumentException: If the URL is not valid.
            Exception: For any other unexpected errors making the page.
        """
        logger.info(f"Getting URL {url}...")
        self.driver.get(url)
        logger.info(f"URL ok.")

    @try_except(exception=[WebDriverException, 
                           StaleElementReferenceException, 
                           TimeoutException])
    def _check_if_interactable(self, xpath: str, wait_time: int, poll_frequency: float) -> None:
        """
        Check if elements located by the given XPath are interactable (clickable).

        Args:
            xpath (str): The XPath used to locate the elements.
            wait_time (int): Maximum time to wait for the elements to become clickable, in seconds.
            poll_frequency (float): How often to check if the elements are clickable, in seconds.

        Raises:
            TimeoutException: If the elements are not clickable within the specified wait time.
            StaleElementReferenceException: If the element becomes stale during the wait.
            WebDriverException: For other WebDriver-related exceptions.
        """
        WebDriverWait(self.driver,
                    wait_time,
                    poll_frequency=poll_frequency).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        return

    @try_except(exception=[WebDriverException, 
                           StaleElementReferenceException, 
                           NoSuchElementException, 
                           TimeoutException])
    def _wait_to_load(self, xpath: str, wait_time: int, poll_frequency: float) -> list[WebElement]:
        """
        Wait for elements specified by the given XPath to be present on the page.

        Args:
            xpath (str): The XPath used to locate the elements on the page.
            wait_time (int): Maximum time to wait for the elements to be present, in seconds.
            poll_frequency (float): How often to check for the presence of the elements, in seconds.

        Returns:
            list[WebElement]: A list of WebElements that match the provided XPath.

        Raises:
            WebDriverException: If there's an issue with the WebDriver during the wait.
            StaleElementReferenceException: If the element becomes stale during the wait.
            NoSuchElementException: If no elements are found matching the XPath after the wait time.
            TimeoutException: If the wait time is exceeded before the elements are found.
        """
        # Wait for the element to load.
        elements = WebDriverWait(self.driver,
                                wait_time,
                                poll_frequency=poll_frequency).until(
            EC.presence_of_all_elements_located((By.XPATH, xpath))
        )
        return elements if elements else []


    def wait_for_and_then_return_elements(self, 
                                         xpath: str, 
                                         wait_time: int = 10, 
                                         poll_frequency: float = 0.5, 
                                         retries: int = 2
                                         ) -> list[WebElement]:
        """
        Wait for elements to be present and interactable on the page, then return them.

        Args:
            xpath (str): The XPath to locate the elements.
            wait_time (int, optional): Maximum time to wait for the elements, in seconds. Defaults to 10.
            poll_frequency (float, optional): How often to check for the elements, in seconds. Defaults to 0.5.

        Returns:
            list[WebElement]: A list of WebElements that match the XPath and are interactable. 

        Raises:
            TimeoutException: If the elements are not found or not interactable within the specified wait time.
            StaleElementReferenceException: If the element becomes stale during the wait. The method will retry in this case.
            NoSuchElementException: If no elements are found matching the XPath.
            Exception: For any other unexpected errors during the wait process.
        """
        assert retries > 0
        counter = 0
        while counter < retries:
            try:
                # Wait for the element to load.
                elements = self._wait_to_load(xpath, wait_time, poll_frequency)
                if not elements:
                    logger.warning(f"No elements found for x-path '{xpath}'.\nReturning empty list...")
                    return []

                # Check to see if the element is interactable.
                self._check_if_interactable(xpath, wait_time, poll_frequency)

                return elements
            except:
                counter += 1
        logger.exception(f"Could not locate elements after {counter + 1} retries.\nReturning empty list...")
        return []


    @try_except(exception=[WebDriverException, NoSuchElementException], raise_exception=False)
    def find_elements_by_xpath(self, 
                               url: str, 
                               xpath: str, 
                               first_elem: bool=True
                               ) -> WebElement|list[WebElement]|None:
        """
        Find an element or elements on the page using the specified XPath.

        Args:
            url (str): The URL being searched (for logging purposes).
            xpath (str): The XPath to use for finding elements.
            first_elem (bool, optional): If True, returns only the first matching element. 
                                         If False, returns all matching elements. Defaults to True.

        Returns:
            WebElement|list[WebElement]|None: 
                - If first_elem is True: Returns the first matching WebElement, or None if not found.
                - If first_elem is False: Returns a list of matching WebElements, or None if none found.

        Raises:
            NoSuchElementException: If no elements are found and first_elem is True.
            WebDriverException: For other Selenium-related errors.
        """
        logger.info(f"Searching for {'first element' if first_elem else 'all elements'} along x-path '{xpath}'")
        elements: WebElement = self.driver.find_element(by=By.XPATH, value=xpath)
        if not elements:
            logger.warning(f"No elements found for URL '{url}'.\n Check the x-path '{xpath}'.\nReturning None...")
            return None

        elements = elements if first_elem else self.driver.find_elements(by=By.XPATH, value=xpath)
        return elements


    @try_except(exception=[WebDriverException])
    def press_buttons(self, 
                      xpath: str, 
                      first_button: bool = True, 
                      delay: float = 0.5,
                      target_buttons: list[str] = None,
                      ) -> None:
        """
        Press one or multiple buttons identified by the given XPath.
        TODO Add in logic to specify which buttons to press.

        Args:
            xpath (str): The XPath used to locate the button(s) on the page.
            first_button (bool, optional): If True, only the first button found will be clicked.
                                           If False, all matching buttons will be clicked. 
                                           Defaults to True.
            delay (float, optional): Wait between button clicks. Defaults to 0.5 (half a second)
            targt_buttons(list[str], optional): A list specifying which buttons to press if they are found. Defaults to None.

        Raises:
            WebDriverException: If there's an issue with the WebDriver while attempting to click.
        """
        # Find all the buttons
        buttons = self.driver.find_elements(By.XPATH, xpath)
        if not buttons:
            logger.warning(f"No buttons found for XPath: {xpath}")
            return

        # Click on all or a specified set of buttons.
        buttons_to_click = buttons[:1] if first_button else buttons
        for button in buttons_to_click:
            if target_buttons:
                if button.text in target_buttons:
                    button.click()
            else:
                button.click()
            time.sleep(delay)  # Wait between clicks
        return

class GetMunicodeSidebarElements(GetElementsWithSelenium):
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


    def _get_current_code_version(self, url: str) -> str:
        """
        Get the current version of the code.
        """
        result: WebElement = self.find_elements_by_xpath(url, xpath=self.xpath_dict['current_version'])
        logger.debug(f'code_verion: {result.text}')
        return result.text.strip()


    def _get_all_code_versions(self, url: str) -> list[str]:
        """
        Get the current and past versions of the code.
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


    def _get_code_urls(self, 
                       url: str, 
                       input_list: list, 
                       wait_time: int=5
                       ) -> list[dict[str, bool]]:
        """
        Get the table of contents (toc) for a code on Municode.
        This is what we want at the end of the day.
        NOTE Municode's toc are recursive, so we need to 'walk' through the toc node tree to get all the URLs.
        Example X-path: //*[@id="genToc_AGHIMUCO"]
        """
        has_children = "::node.HasChildren"

        # Get all the current toc elements
        elements: list[WebElement] = self.find_elements_by_xpath(url, self.xpath_dict['toc'], first_elem=False)
        logger.info(f"Got table of contents elements for URL {url}. Checking...")

        # Get their URLs, text, and whether or not they have subnodes
        urls = [
            {
             "href": element.get_attribute("href"),
             "text": element.text.strip(),
             "has_children": bool(has_children in element.get_attribute("ng-if"))
            } for element in elements
        ]
        logger.info(f"Found {len(urls)} elements for URL '{url}'. Appending to input_list...")
        logger.debug(f"urls\n{urls}")
        input_list.append(urls)

        # If it has subnodes, click on them, wait for them to load, and run the function again.
        # Else, skip it.
        # NOTE Municode sidebar elements only go 3-4 levels deep, so we shouldn't have to worry about recurrsion depth here.
        for url in urls:
            if url["has_children"] is True:
                logger.info(f"URL '{url}' has child nodes. Pressing the button then trying again.")
                self.press_buttons(f"{self.xpath_dict['toc']}/button")
                time.sleep(wait_time)
                self._get_code_urls(url, input_list)
            else:
                continue
        return urls, input_list


    def _skip_if_we_have_url_already(self, url: str) -> dict|None:
        """
        Check if we already have a CSV file of the input URL. 
        If we do, load it as a dictionary and return it. Else, return None
        """
        url_filename = sanitize_filename(url)
        url_file_path = os.path.join(os.getcwd(), f"{url_filename}.csv")
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
                                      row: NamedTuple
                                      ) -> list[dict]:
        """
        Extract the code versions and table of contents from a city's Municode page.
        NOTE This function orchestrates all the methods of this class, similar to main.py
        """
        # Check to make sure the URL is a municode one, then initialize the dictionary.
        assert "municode" in url, f"URL '{url}' is not for municode."
        url = row.url

        # Skip the webpage if we already got it.
        output_dict = self._skip_if_we_have_url_already(url)
        if output_dict:
            return output_dict
        else:
            output_dict = {}

        # Open the webpage.
        self.make_page(url)

        # Get the date the code was last updated.
        output_dict['current_code_version'] = self._get_current_code_version(url)

        # Get all the previous dates the code was updated.
        # NOTE This does not grab their links, just their text.
        all_code_versions: list[str] = self._get_all_code_versions(url)
        output_dict['all_code_versions'] = all_code_versions

        # Get the URLs from the table of contents.
        # NOTE The tables are recursive, so this won't get all the buried URLs. 
        input_list = []
        _, input_list = self._get_code_urls(url, input_list)
        output_dict["table_of_contents_urls"] = input_list

        logger.info(f"Found all elements from Municode URL for gnis '{row.gnis}'")
        # Export output_dict as a CSV file for safe-keeping.
        save_to_csv(output_dict)

        return output_dict




def get_sidebar_urls_from_municode_with_selenium(df: pd.DataFrame, wait_in_seconds: int) -> list[dict]:
    """
    Get href and text of sidebar elements in a Municode city code URL.
    """

    # Initialize webdriver.
    options = {} # Empty for the moment.
    driver = webdriver.Chrome(options=options)

    # Get the sidebar URLs and text for each Municode URL
    # NOTE Adding the 'if row else None' is like adding 'continue' to a regular for-loop.
    with GetMunicodeSidebarElements(wait_in_seconds=wait_in_seconds, driver=driver) as municode:

        # Get the side bar elements for each Municode URL.
        list_of_lists_of_dicts: list[dict] = [ 
            municode.get_municode_sidebar_elements(row.url) if row else None for row in df.itertuples()
        ]

        # Flatten the list of lists of dictionaries into just a list of dictionaries.
        output_list = [item for sublist in list_of_lists_of_dicts for item in sublist]

    return output_list



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