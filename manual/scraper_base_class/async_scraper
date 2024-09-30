
import asyncio
from typing import Any


from playwright.async_api import (
    Playwright as AsyncPlaywright,
    Browser as AsyncPlaywrightBrowser,
    Page as AsyncPlaywrightPage,
    TimeoutError as AsyncPlaywrightTimeoutError,
)

from utils.manual.scrape_legal_websites_utils.extract_urls_using_javascript import async_extract_urls_using_javascript
from utils.manual.scrape_legal_websites_utils.fetch_robots_txt import async_fetch_robots_txt
from utils.manual.scrape_legal_websites_utils.parse_robots_txt import parse_robots_txt 
from utils.manual.scrape_legal_websites_utils.can_fetch import can_fetch

from config import LEGAL_WEBSITE_DICT, CONCURRENCY_LIMIT

from logger import Logger
logger = Logger(logger_name=__name__)


class AsyncScraper:

    def __init__(self, 
                 pw_instance: AsyncPlaywright, 
                 robot_txt_url: str, 
                 user_agent: str="*", 
                 **launch_kwargs):
        self.launch_kwargs = launch_kwargs
        self.legal_website_dict: dict = LEGAL_WEBSITE_DICT
        self.pw_instance: AsyncPlaywright = pw_instance
        self.user_agent: str = user_agent
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self.robot_txt_url: str = robot_txt_url
        self.browser: AsyncPlaywrightBrowser = None
        self.robot_rules: dict[str,dict[str,Any]]  = {}
        self.source: str = None # NOTE Assinged in site-specific scraper class.


    #### START CLASS STARTUP AND EXIT METHODS ####
    async def async_get_robot_rules(self, robot_txt_url) -> None:
        """
        Asynchronously Get the site's robots.txt file and assign it to the robot_urls attribute
        """
        robots_txt = await async_fetch_robots_txt(robot_txt_url)
        rules: dict[str,dict[str|Any]] = parse_robots_txt(robots_txt)
        self.robot_rules = rules


    async def _async_load_browser(self) -> None:
        """Asynchronously launch a chromium instance and load a page"""
        self.browser = await self.pw_instance.chromium.launch(**self.launch_kwargs)


    async def _async_close_browser(self) -> None:
        """Close browser instance and reset internal attributes"""
        if self.browser:
            await self.browser.close()
            self.browser = None


    @classmethod
    async def async_start(cls, pw_instance, robot_txt_url, user_agent, **launch_kwargs) -> 'AsyncScraper':
        instance = cls(pw_instance, robot_txt_url, user_agent=user_agent, **launch_kwargs)
        await instance._async_load_browser()
        await instance.async_get_robot_rules(instance.robot_txt_url)
        return instance


    async def async_close(self) -> None:
        await self._async_close_browser()


    async def __aenter__(self) -> 'AsyncScraper':
        await self._async_load_browser()
        await self.async_get_robot_rules(self.robot_txt_url)
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.async_close()

    #### END CLASS STARTUP AND EXIT METHODS ####


    #### START PAGE PROCESSING METHODS ####
    async def _async_instantiate_webpage(self) -> AsyncPlaywrightPage:
        """
        Instantiate an AsyncPlaywrightPage within a browser context.
        """
        context = await self.browser.new_context()
        return await context.new_page()


    async def _async_open_webpage(self, url: str) -> AsyncPlaywrightPage:
        """
        Open a specified webpage and wait for any dynamic elements to load.
        """
        page: AsyncPlaywrightPage = await self._async_instantiate_webpage()
        await page.goto(url)
        await page.wait_for_load_state("networkidle") # NOTE Defaults to 30 seconds wait time.
        return page


    async def _async_fetch_urls_from_page(self, url: str) -> dict[str] | dict[None]:
        """
        Fetch URLs from a webpage.
        """
        page: AsyncPlaywrightPage = await self._async_open_webpage(url)
        url_dict =  await async_extract_urls_using_javascript(page, self.source)
        page.close()
        return url_dict


    async def _async_respectful_fetch(self, url: str) -> dict[str, str] | None:
        """
        Limit scraping a URL based on a semaphore and the delay specified in robots.txt
        """
        async with self.semaphore:
            # See if we're allowed to get the URL, as well get the specified delay from robots.txt
            fetch, delay = can_fetch(url, self.robot_rules)
            if not fetch:
                logger.warning(f"Cannot scrape URL '{url}' as it's disallowed in robots.txt")
                return None
            else:
                await asyncio.sleep(delay)
                return await self._async_fetch_urls_from_page(url)


    async def async_scrape(self, url: str) -> dict[str, str]|dict[None]:
        """
        Scrape a URL asynchronously
        """
        try:
            return await self._async_respectful_fetch(url)
        except AsyncPlaywrightTimeoutError as e:
            logger.info(f"url '{url}' timed out.")
            logger.debug(e)
            return {}

