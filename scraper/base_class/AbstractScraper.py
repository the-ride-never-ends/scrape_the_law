import time
from typing import Any, TypeVar, overload, NamedTuple

import requests
from requests import (
    HTTPError,
    RequestException
)
import aiohttp

import pandas as pd
from playwright.sync_api import (
    Playwright,
    Browser as PlaywrightBrowser,
    Page as PlaywrightPage,
    TimeoutError as PlaywrightTimeoutError,
)

from abc import ABC, abstractmethod

from utils.manual.scrape_legal_websites_utils.fetch_robots_txt import fetch_robots_txt
from utils.manual.scrape_legal_websites_utils.parse_robots_txt import parse_robots_txt 
from utils.manual.scrape_legal_websites_utils.extract_urls_using_javascript import extract_urls_using_javascript
from utils.manual.scrape_legal_websites_utils.can_fetch import can_fetch
from utils.shared.decorators.try_except import try_except

from config import LEGAL_WEBSITE_DICT

from logger import Logger
logger = Logger(logger_name=__name__)

pd.DataFrame

AbstractPage = TypeVar('AbstracPage')
AbstractBrowser= TypeVar('AbstractBrowser')
AbstractScraper = TypeVar('AbstractScraper')
AbstractInstanceOrDriver = TypeVar('AbstractInstanceOrDriver')

from urllib.robotparser import RobotFileParser
from urllib.error import URLError
from urllib.parse import urljoin

class AbstractScraper(ABC):
    """
    Abstract class for a JS-friendly webscraper.
    Designed for 5 child classes: Sync Playwright, Async Playwright, Selenium, Requests, and Aiohttp.

    Parameters:
        instance_or_driver: An instance or driver
        robot_txt_url: URL path to a website's robots.txt page.
        user_agent: The chosen user agent in robots.txt. Defaults to '*'
        **launch_kwargs:
            Keyword arguments to be passed to an instance or driver
            For example, you can pass ``headless=False, slow_mo=50`` 
            for a visualization of a search engine search to `playwright.chromium.launch`.
    """
    def __init__(self, 
                 domain: str,
                 instance_or_driver: AbstractInstanceOrDriver = None,
                 user_agent: str="*", 
                 **launch_kwargs):
        self.launch_kwargs = launch_kwargs
        self.instance_or_driver: AbstractInstanceOrDriver = instance_or_driver
        self.domain: str = domain
        self.user_agent: str = user_agent
        self.rp: RobotFileParser = RobotFileParser()
        self.browser: AbstractBrowser = None
        self.crawl_delay: int = None
        self.rrate: NamedTuple = None

        if not instance_or_driver:
            raise ValueError("Driver or Instance missing from scraper keyword arguments")

    #### START CLASS STARTUP AND EXIT METHODS ####

    @try_except(exception=[URLError], retries=2, raise_exception=True)
    def get_robot_rules(self):
        """
        Get the site's robots.txt file and assign it to the class' applicable attributes
        See: https://docs.python.org/3/library/urllib.robotparser.html
        """
        # Construct the URL to the robots.txt file
        robots_url = urljoin(self.domain, 'robots.txt')
        self.rp.set_url(robots_url)

        # Read the robots.txt file from the server
        self.rp.read()

        # Set the request rate.
        self.rrate = self.rp.request_rate(self.user_agent)

        # Set the crawl delay
        self.crawl_delay = int(self.rp.crawl_delay(self.user_agent))
        return

    @abstractmethod
    def __enter__(self) -> AbstractScraper:
        self.get_robot_rules(self.robot_txt_url)

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass


    @classmethod
    def start(cls, domain, instance_or_driver, user_agent, **launch_kwargs) -> AbstractScraper:
        instance = cls(domain, instance_or_driver=instance_or_driver, user_agent=user_agent, **launch_kwargs)
        instance.get_robot_rules(instance.robot_txt_url)

    def close(self) -> None:
        self._close_browser()

    def _load_browser(self) -> None:
        """
        Launch a browser.
        """
        pass

    @abstractmethod
    def _close_browser(self) -> None:
        """
        Close browser instance and reset internal attributes
        """
        pass

    #### END CLASS STARTUP AND EXIT METHODS ####


    #### START PAGE PROCESSING METHODS ####
    def create_page(self) -> AbstractPage:
        context = self.browser.new_context()
        page = context.new_page()
        return page


    def get_robot_rules(self):
        """
        Get the site's robots.txt file and assign it to the self.robot_urls attribute
        """
        robots_txt = fetch_robots_txt(self.robot_txt_url)
        rules = parse_robots_txt(robots_txt)
        self.robot_rules = rules


    def _open_webpage(self, url: str, page: PlaywrightPage) -> None:
        """
        Open a specified webpage and wait for any dynamic elements to load.
        """
        # See if we're allowed to get the URL, as well get the specified delay from robots.txt
        fetch, delay = can_fetch(url, self.robot_rules)
        if not fetch:
            logger.warning(f"Cannot scrape URL '{url}' as it's disallowed in robots.txt")
            return
        else:
            time.sleep(delay)
            page.goto(url)
            page.wait_for_load_state("networkidle")
            return page


    def _fetch_urls_from_page(self, url: str) -> dict[str]|dict[None]:
        page: PlaywrightPage = self._open_webpage(url)
        urls_dict: dict = extract_urls_using_javascript(page, self.source)
        return urls_dict


    def _respectful_fetch(self, url: str) -> dict[str]|dict[None]:
        """
        Limit getting URLs based on a semaphore and the delay specified in robots.txt
        """
        fetch, delay = can_fetch(url)
        if not fetch:
            logger.warning(f"Cannot scrape URL '{url}' as it's disallowed in robots.txt")
            return None
        else:
            time.sleep(delay)
            return self._fetch_urls_from_page(url)
