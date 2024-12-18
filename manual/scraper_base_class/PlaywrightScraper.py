import time
from typing import Any

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

from config.config import LEGAL_WEBSITE_DICT

from logger.logger import Logger
logger = Logger(logger_name=__name__)



class Scraper(ABC):
    """
    Use Playwright to scrape a webpage of URLs.

    Parameters:
        pw_instance: A synchronous Playwright instance.
        robot_txt_url: URL path to a website's robots.txt page.
        current_agent: The chosen user agent in robots.txt. Defaults to '*'
        **launch_kwargs:
            Keyword arguments to be passed to
            `playwright.chromium.launch`. For example, you can pass
            ``headless=False, slow_mo=50`` for a visualization of the
            search.
    """
    def __init__(self, 
                 pw_instance: Playwright, 
                 robot_txt_url: str, 
                 current_agent: str="*", 
                 **launch_kwargs):
        self.launch_kwargs = launch_kwargs
        self.legal_website_dict: dict = LEGAL_WEBSITE_DICT
        self.pw_instance: Playwright = pw_instance
        self.current_agent: str = current_agent
        self.robot_txt_url: str = robot_txt_url
        self.browser: PlaywrightBrowser = None
        self.robot_rules: dict[str,dict[str,Any]] = {}
        self.source: str = None

    #### START CLASS STARTUP AND EXIT METHODS ####


    def __enter__(self) -> 'Scraper':
        self._load_browser()
        self.get_robot_rules(self.robot_txt_url)
        return self


    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._close_browser()


    @classmethod
    def start(cls, pw_instance, current_agent, robot_txt_url, **launch_kwargs) -> 'Scraper':
        instance = cls(pw_instance, robot_txt_url, current_agent=current_agent, **launch_kwargs)
        instance._load_browser()
        instance.get_robot_rules(instance.robot_txt_url)
        return instance


    def close(self) -> None:
        self._close_browser()


    def _load_browser(self) -> None:
        """Launch a chromium instance and load a page"""
        self.browser = self.pw_instance.chromium.launch(**self.launch_kwargs)


    def _close_browser(self) -> None:
        """Close browser instance and reset internal attributes"""
        if self.browser:
            self.browser.close()
            self.browser = None
    #### END CLASS STARTUP AND EXIT METHODS ####


    #### START PAGE PROCESSING METHODS ####
    def create_page(self) -> PlaywrightPage:
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


    # def scrape(self, url: str) -> dict[str]|dict[None]:
    #     try:
    #         return self._respectful_fetch(self, url)
    #     except PlaywrightTimeoutError as e:
    #         logger.info(f"url '{url}' timed out.")
    #         logger.debug(e)
    #         return {}


