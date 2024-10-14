import asyncio
import os
import random
import time
import traceback

import abc
from typing import Any, Callable

import pandas as pd

import selenium
import selenium.webdriver
from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    WebDriverException, 
    NoSuchElementException,
    TimeoutException, 
    StaleElementReferenceException, 
    NoSuchElementException
)


from utils.shared.sanitize_filename import sanitize_filename
from ..manual.link_urls_to_location_data.main.load_from_csv import load_from_csv
from ..manual.link_urls_to_location_data.main.save_to_csv import save_to_csv

from config import LEGAL_WEBSITE_DICT
from logger import Logger
logger = Logger(logger_name=__name__)


class GetElementsWithPlaywright:
    pass


class GetElementsWithSelenium:
    def __init__(self, robots_txt: int=1):
        self.driver = webdriver.Chrome()
        self.robots_txt = robots_txt

    def _close_webpage(self):
        return self.driver.close()
    
    def _quit_driver(self):
        return self.driver.quit()

    def __enter__(self):
        return self

    @classmethod
    def enter(cls, robots_txt):
        """
        Factory method to start Selenium
        """
        instance = cls(robots_txt)
        return instance

    def __exit__(self):
        self._close_webpage()
        self._quit_driver()
        return

    def exit(self):
        """
        Manually close the webpage and webdriver.
        """
        self._close_webpage()
        self._quit_driver()
        return

    def make_page(self, url: str) -> webdriver.Chrome:
        """
        Navigate to the specified URL using the given Chrome webdriver.

        Args:
            url (str): The URL to navigate to.
            driver (webdriver.Chrome): The Chrome webdriver instance.

        Returns:
            webdriver.Chrome: The Chrome webdriver after navigation.
        """
        logger.info(f"Getting URL {url}...")
        self.driver.get(url)
        logger.info(f"URL ok.")
        return

    def wait_for_and_then_element(self, x_path: str, wait_time: int = 10, poll_frequency: float = 0.5) -> list[WebElement]:
        """
        Wait for elements to be present and interactable on the page, then return them.

        Args:
            x_path (str): The XPath to locate the elements.
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
        try:
            elements = WebDriverWait(self.driver, wait_time, poll_frequency=poll_frequency).until(
                EC.presence_of_all_elements_located((By.XPATH, x_path))
            )
            # Additional check to ensure elements are actually interactable
            WebDriverWait(self.driver, wait_time, poll_frequency=poll_frequency).until(
                EC.element_to_be_clickable((By.XPATH, x_path))
            )
            return elements
        except TimeoutException:
            logger.error(f"Timeout waiting for elements with XPath: {x_path}")
            traceback.print_exc()
        except StaleElementReferenceException:
            logger.warning(f"Stale element reference for XPath: {x_path}. Retrying...")
            traceback.print_exc()
            return self.wait_for_element(x_path, wait_time, poll_frequency)
        except NoSuchElementException:
            logger.error(f"No such element found with XPath: {x_path}")
            traceback.print_exc()
        except Exception as e:
            logger.error(f"Unexpected error waiting for elements with XPath {x_path}: {e}")
            traceback.print_exc()


    def find_elements_by_xpath(self, url: str, xpath: str, first_elem: bool=True) -> WebElement|list[WebElement]|None:
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
            selenium.common.exceptions.NoSuchElementException: If no elements are found and first_elem is True.
            selenium.common.exceptions.WebDriverException: For other Selenium-related errors.
        """
        if first_elem:
            logger.info(f"Searching for first element along x-path '{xpath}'")
            try:
                elements: WebElement = self.driver.find_element(by=By.XPATH, value=xpath)
            except NoSuchElementException:
                logger.exception(f"Element not found along x-path for url '{url}'\nxpath:{xpath}")
            except WebDriverException as e:
                logger.exception(f"Unknown webdriver exception finding element: {e}")
        else:
            try:
                logger.info(f"Searching for all elements along x-path '{xpath}'")
                elements: list[WebElement] = self.driver.find_elements(by=By.XPATH, value=xpath)
            except NoSuchElementException:
                logger.exception(f"Elements not found along x-path for url '{url}'\nxpath:{xpath}")
                traceback.print_exc()
            except WebDriverException as e:
                logger.exception(f"Unknown webdriver exception finding elements: {e}")
                traceback.print_exc()

        if len(elements) == 0:
            logger.warning(f"No elements found for URL {url}. Check the x-path. Returning...")
            return
        else:
            return elements


    def press_buttons(self, xpath: str, first_button: bool = True, delay: float = 0.5) -> None:
        """
        Press one or multiple buttons identified by the given XPath.

        Args:
            xpath (str): The XPath used to locate the button(s) on the page.
            first_button (bool, optional): If True, only the first button found will be clicked.
                                           If False, all matching buttons will be clicked. 
                                           Defaults to True.
            delay (float, optional): Wait between button clicks. Defaults to 0.5 (half a second)

        Raises:
            WebDriverException: If there's an issue with the WebDriver while attempting to click.
        """
        try:
            if first_button:
                self.driver.find_element(By.XPATH, xpath).click()
                return
            else:
                buttons = self.driver.find_elements(By.XPATH, xpath)
                for button in buttons:
                    button.click()
                    time.sleep(delay)  # Wait between clicks
                return
        except WebDriverException as e:
            logger.exception(f"Webdriver exception clicking on button: {e}")
            traceback.print_exc()


    def apply_dict(self, input_dict: dict[dict[str, Callable]]) -> Any:
        """
        Apply a dictionary of functions and function inputs in succession.

        Args:
            input_dict (dict[dict[str, Callable]]): A dictionary where each key is a step name,
                and each value is another dictionary containing:
                - 'func': The function to be called
                - 'args': A tuple of positional arguments for the function
                - 'kwargs': A dictionary of keyword arguments for the function
        Returns:
            Any: The result of applying all functions in the input dictionary

        Raises:
            KeyError: If the input dictionary is not properly formatted.
            TypeError: If the provided 'func' is not callable.

        Example:
            >>> def add(num_1, num_2, num_3, num_kwarg=1):
            >>>     return num_1 + num_2 + num_3 + num_kwarg
            >>> input_dict = {
            >>>     "step": {
            >>>         "func": add,
            >>>         "args": (1, 2, 3),
            >>>         "kwargs": {'num_kwarg': 1}
            >>>     }
            >>> }
            >>> return apply_dict(input_dict)
            7
        """
        result = None
        for key, value in input_dict.items():
            logger.info(f"Applying function {key} with args {value["args"]} and kwargs{value["kwargs"]}...")
            logger.info(f"Functions\n{key}")
            for key, value in value.items():
                func = value["func"]
                args = value["args"]
                kwargs = value["kwargs"]
                try:
                    result = func(result, *args, **kwargs)
                except KeyError:
                    logger.error("input_dict is not formated correctly.")
                except TypeError:
                    logger.error(f"input_dict function '{func}' is not callable")
                except Exception as e:
                    logger.exception(f"Unknown error: {e}")
        return result



class GetMunicodeElements(GetElementsWithSelenium):

    def __init__(self, driver, *args, **kwargs):
        super().__init__(driver, *args, **kwargs)
        # self.driver = driver
        # self.instance = None

        # if isinstance(driver, selenium.webdriver):
        #     self.instance = GetElementsWithSelenium(driver, *args, **kwargs)
        # else:
        #     self.instance = GetElementsWithPlaywright(driver, *args, **kwargs)

    # # called when an attribute is not found:
    # def __getattr__(self, name):
    #     # assume it is implemented by self.instance
    #     return self.instance.__getattribute__(name)

    # # called when a length is not found:
    # def __len__(self, name):
    #     # assume it is implemented by self.instance
    #     return self.instance.__len__(name)

    # # called when a getitem is not found:
    # def __getitem__(self, name):
    #     # assume it is implemented by self.instance
    #     return self.instance.__getitem__(name)


    def _get_current_code_version(self, url: str) -> str:
        """
        Get the current version of the code.
        """
        current_version_xpath = '//*[@id="codebankToggle"]/button/text()'
        result: WebElement = self.find_elements_by_xpath(url, xpath=current_version_xpath)
        logger.debug(f'code_verion: {result.text}')
        return result.text.strip()


    def _get_all_code_versions(self, url: str) -> str:
        """
        Get the current and past versions of the code.
        """
        version_button_xpath = '//*[@id="codebankToggle"]/button/'
        version_text_paths = "//mcc-codebank//ul/li//button/text()"

        # Press the button that shows the code archives pop-up
        self.press_buttons(url, xpath=version_button_xpath)
        # parent: WebElement = self.driver.find_element(By.ID, "codebank")

        # Get all the dates in the pop-up.
        # parent = self.driver.find_element(By.XPATH, "//mcc-codebank")
        # buttons = parent.find_elements(By.XPATH, ".//ul/li//button/")
        buttons = self.wait_for_and_then_element(
            version_text_paths, wait_time=10, poll_frequency=0.5
        )
        version_list = [
            button.text.strip() for button in buttons
        ]
        logger.debug(f'version_list\n{version_list}',f=True)
        return version_list


    def _get_code_urls(self, url: str, input_list: list) -> list[tuple[str, str, bool]]:
        """
        Get the table of contents for a code on Municode.
        This is what we want at the end of the day.
        NOTE The table of contents are recursive.
        Example X-path: //*[@id="genToc_AGHIMUCO"]
        """
        # Define x-paths
        x_path = "//input[starts-with(@id, 'genToc_')]"
        x_path_button = x_path + "/button"

        # Get all the current table of contents elements
        elements: list[WebElement] = self.find_elements_by_xpath(url, x_path, first_elem=False)
        logger.info(f"Got table of contents elements for URL {url}. Checking...")

        # Get their URLs, text, and whether or not they have subnodes
        urls = [
            {
             "href": element.get_attribute("href"),
             "text": element.text.strip(),
             "has_children": bool("::node.HasChildren" in element.get_attribute("ng-if"))
            } for element in elements
        ]
        logger.info(f"Found {len(urls)} elements for URL '{url}'")
        logger.info(f"Found {len(urls)} elements for URL '{url}'")

        # If it has subnodes, click on them and run the function again.
        for url in urls:
            if url["has_children"] is True:
                self.press_buttons(x_path_button)
                self._get_code_urls(url)
            else:
                continue
        return urls


    def get_municode_elements(self, url: str, wait_in_seconds: int) -> str:
        """
        Extract the code versions and table of contents from a city's Municode page.
        NOTE This function is the primary purpose of this class.
        """
        # Check to make sure the URL is a municode one, then initialize the dictionary.
        assert "municode" in url, f"URL '{url}' is not for municode."
        output_dict = {}

        # Open the webpage.
        self.driver.refresh()
        self.make_page(url)

        # Get the date the code was last updated.
        output_dict['current_code_version'] = self._get_current_code_version(url)

        # Get all the previous dates the code was updated.
        # NOTE This does not grab their links, just their text.
        all_code_versions: list[str] = self._get_all_code_versions(url)
        output_dict['all_code_versions'] = all_code_versions

        # Get the URLs from the table of contents.
        # NOTE The tables are recursive, so this won't get all the buried URLs. 
        # However, URLs with nodes link to table of contents that have all the URLs we need on them anyways, so we just need to scrape that in turn.
        input_list = []
        urls = self._get_code_urls(url, input_list)
        output_dict["table_of_contents_urls"] = urls
        for url in urls:
            if url['has_children']:
                self._get_code_urls(url)
        


async def get_urls_with_selenium(df: pd.DataFrame):

    for row in df.itertuples():
        _get_urls_with_selenium(row.url)

async def _get_urls_with_selenium(url: str, 
                                driver: webdriver.Chrome,
                                wait_in_seconds: int, 
                                sleep_length: int=5,
                                source: str=None,
                                class_: str=None,
                                allow_redirects: bool=True,) -> list[dict[str, str]]:
    """
    Get a list of places (towns/counties)

    Args:
        url (str): URL to scrape
        driver (webdriver.Chrome): A Chromium webdriver

    Returns:
        data_out (pd.DataFrame): a dataframe of towns by state
    """
    if not source:
        raise ValueError("source argument not specified.")

    url_filename = sanitize_filename(url)
    url_file_path = os.path.join(os.getcwd(), f"{url_filename}.csv")
    class_ = class_ or LEGAL_WEBSITE_DICT[source]['target_class']
    xpath_init = f"//a[@class='{class_}']"

    # assert that the driver is webdriver.Chrome
    assert isinstance(driver, webdriver.Chrome)

    if os.path.exists(url_file_path):
        logger.info(f"Got URL '{url}' already. Loading csv...")
        results = load_from_csv(url_file_path)
        return results
    else:
        driver.refresh()
        logger.info(f"Waiting {wait_in_seconds} seconds per {source}'s robots.txt")
        time.sleep(wait_in_seconds)

    logger.info(f"Getting URL {url}...")
    driver.get(url)
    logger.info(f"URL ok. Searching for elements along x-path '{xpath_init}'")

    elements = driver.find_elements(by=By.XPATH, value=xpath_init)
    if len(elements) == 0:
        logger.warning(f"No elements found for URL {url}. Check the x-path. Returning...")
        return

    # logger.debug(f"elements: {elements}")

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