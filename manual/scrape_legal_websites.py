

import asyncio
from contextlib import contextmanager, asynccontextmanager, AsyncExitStack
import sys
from typing import Any, NamedTuple

import pandas as pd
from utils.shared.next_step import next_step


from playwright.async_api import (
    async_playwright,
    Playwright as AsyncPlaywright,
    Browser as AsyncPlaywrightBrowser,
    Page as AsyncPlaywrightPage,
    TimeoutError as AsyncPlaywrightTimeoutError,
)

from playwright.sync_api import (
    sync_playwright,
    Playwright,
    Browser as PlaywrightBrowser,
    Page as PlaywrightPage,
    TimeoutError as PlaywrightTimeoutError,
)

import re
from urllib.parse import ParseResult
from utils.manual.scrape_legal_websites.extract_urls_using_javascript import extract_urls_using_javascript


from database import MySqlDatabase

from config import LEGAL_WEBSITE_DICT, CONCURRENCY_LIMIT, HEADLESS

from logger import Logger
logger = Logger(logger_name=__name__)

from utils.shared.return_s_percent import return_s_percent

sql_commands = {
    "get_locations":"""
                    SELECT gnis, place_name, class_code, state_code 
                    WHERE domain_name IS NOT NULL;
                    """,
    "insert_into_searches": """
                            INSERT INTO {table} ({column_names}) VALUES {values}
                            ON DUPLICATE KEY UPDATE  
                            """,
}

import asyncio
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
import aiohttp

from utils.manual.scrape_legal_websites.fetch_robots_txt import fetch_robots_txt, async_fetch_robots_txt
from utils.manual.scrape_legal_websites.parse_robots_txt import parse_robots_txt 

class Scraper:

    def __init__(self, pw_instance: AsyncPlaywright|Playwright, robot_txt_url: str, user_agent: str="*", **launch_kwargs):
        """
        Parameters
        ----------
        pw_instance: A Playwright instance. May be either synchronous or asynchronous.
        **launch_kwargs
            Keyword arguments to be passed to
            `playwright.chromium.launch`. For example, you can pass
            ``headless=False, slow_mo=50`` for a visualization of the
            search.
        """
        self.launch_kwargs = launch_kwargs
        self.legal_website_dict: dict = LEGAL_WEBSITE_DICT
        self.sql_commands: dict = sql_commands
        self.pw_instance = pw_instance
        self.user_agent = user_agent
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        self.robot_txt_url = robot_txt_url
        self.browser: AsyncPlaywrightBrowser|PlaywrightBrowser = None
        self.robot_rules = {}
        self.source = None

    #### START CLASS STARTUP AND EXIT METHODS ####

    async def __aenter__(self) -> 'Scraper':
        self.exit_stack = AsyncExitStack()
        await self._async_load_browser()
        await self.async_get_robot_rules(self.robot_txt_url)
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.exit_stack.aclose()


    def __enter__(self) -> 'Scraper':
        self._load_browser()
        self.get_robot_rules(self.robot_txt_url)
        return self


    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._close_browser()


    @classmethod
    async def async_start(cls, pw_instance, robot_txt_url, user_agent, **launch_kwargs) -> 'Scraper':
        instance = cls(pw_instance, user_agent=user_agent, **launch_kwargs)
        await instance._async_load_browser()
        await instance.async_get_robot_rules(robot_txt_url)
        return instance


    @classmethod
    def start(cls, pw_instance, user_agent, robot_txt_url, **launch_kwargs) -> 'Scraper':
        instance = cls(pw_instance, user_agent=user_agent, **launch_kwargs)
        instance._load_browser()
        instance.get_robot_rules(robot_txt_url)
        return instance


    async def async_close(self) -> None:
        await self._async_close_browser()


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


    async def _async_load_browser(self) -> None:
        """Asynchronously launch a chromium instance and load a page"""
        self.browser = await self.pw_instance.chromium.launch(**self.launch_kwargs)


    async def _async_close_browser(self) -> None:
        """Close browser instance and reset internal attributes"""
        if self.browser:
            await self.browser.close()
            self.browser = None

    #### END CLASS STARTUP AND EXIT METHODS ####

    #### START ROBOTS.TXT METHODS #### 

    def get_robot_rules(self):
        robots_txt = fetch_robots_txt(self.robot_txt_url)
        rules = parse_robots_txt(robots_txt)
        self.robot_rules = rules


    async def async_get_robot_rules(self):
        robots_txt = async_fetch_robots_txt(self.robot_txt_url)
        rules = parse_robots_txt(robots_txt)
        self.robot_rules = rules


    def can_fetch(self, url: str) -> bool:
        path = urlparse(url).path

        # Check if path matches any allow rule
        for allow_path in self.robot_rules.get('allow', []):
            if re.match(allow_path.replace('*', '.*'), path):
                return True

        # Check if path matches any disallow rule
        for disallow_path in self.robot_rules.get('disallow', []):
            if re.match(disallow_path.replace('*', '.*'), path):
                return False
        # If no rules match, it's allowed by default
        return True

    #### END ROBOTS.TXT METHODS ####

    #### START PAGE PROCESSING METHODS ####

    def create_page(self) -> PlaywrightPage:
        context = self.browser.new_context()
        page = context.new_page()
        return page


    async def async_create_page(self):
        context = await self.browser.new_context()
        page = await context.new_page()
        return page


    def open_webpage(self, scrape_url: str, page: PlaywrightPage) -> None:
        """
        Open a specified webpage and wait for any dynamic elements to load.
        """
        if not self.can_fetch(scrape_url):
            logger.warning(f"Cannot scrape URL '{scrape_url}' as it's disallowed in robots.txt")
            return
        page.goto(scrape_url)
        page.wait_for_load_state("networkidle")
        return page


    async def async_open_webpage(self, scrape_url: str, page: AsyncPlaywrightPage) -> None:
        """
        Open a specified webpage asynchronously and wait for any dynamic elements to load.
        """
        # See if we're allowed to get the URL, as well get the specified delay from robots.txt
        fetch, delay = self.can_fetch(scrape_url)

        if not fetch:
            logger.warning(f"Cannot scrape URL '{scrape_url}' as it's disallowed in robots.txt")
            return
        else:
            await asyncio.sleep(delay)
            await page.goto(scrape_url)
            await page.wait_for_load_state("networkidle")
            return page

    from utils.manual.scrape_legal_websites.extract_urls_using_javascript import extract_urls_using_javascript

    async def get_links(self, url: str):
        page: AsyncPlaywrightPage = await self.async_open_webpage(url)
        urls_dict: dict = await self.extract_urls_using_javascript(page, self.source)
        return urls_dict

    async def async_respectful_fetch(self, url):
        async with self.semaphore:
            fetch, delay = self.can_fetch(url)
            if not fetch:
                logger.warning(f"Cannot scrape URL '{url}' as it's disallowed in robots.txt")
                return None
            else:
                await asyncio.sleep(delay)
                return await self.get_links(url)
  

    #### END PAGE PROCESSING METHODS ####

class GeneralCodeScraper(Scraper):

    def __init__(self, pw_instance, robots_txt_url, **launch_kwargs):
        """
        Parameters
        ----------
        **launch_kwargs
            Keyword arguments to be passed to
            `playwright.chromium.launch`. For example, you can pass
            ``headless=False, slow_mo=50`` for a visualization of the
            search.
        """
        super().__init__(pw_instance, robots_txt_url, **launch_kwargs)

        async def scrape(self) -> pd.DataFrame:
            pass

class AmericanLegalScraper(Scraper):

    def __init__(self, pw_instance, robots_txt_url, **launch_kwargs):
        """
        Parameters
        ----------
        **launch_kwargs
            Keyword arguments to be passed to
            `playwright.chromium.launch`. For example, you can pass
            ``headless=False, slow_mo=50`` for a visualization of the
            search.
        """
        super().__init__(pw_instance, robots_txt_url, **launch_kwargs)

    def _build_state_url(self, row: NamedTuple) -> str:
        base_url = self.legal_website_dict["american_legal"]["base_url"]
        self.source = self.legal_website_dict["american_legal"]["source"]
        path = row.state_code.lower()
        url = f"{base_url}{path}" # -> "https://codelibrary.amlegal.com/regions/az"
        assert len(scrape_url) == 42, f"scrape_url is not 40 characters, but {len(scrape_url)}"
        return url

    async def scrape(self, df: pd.DataFrame, db: MySqlDatabase) -> pd.DataFrame:
        for row in df.itertuples():
            try:
                await self.async_respectful_fetch(self, url)
            except PlaywrightTimeoutError as e:
                print("")


class MunicodeScraper(Scraper):
    """
    Search for top results on google and return their links.\n
    NOTE This has been heavily modified from ELM's original code. We'll see if it's more effective in the long run.

    Parameters
    ----------
    pw_instance: an asynchronous playwright instance.
    **launch_kwargs
        Keyword arguments to be passed to
        `playwright.chromium.launch`. For example, you can pass
        ``headless=False, slow_mo=50`` for a visualization of the
        search.
    """

    def __init__(self, pw_instance, robots_txt_url, **launch_kwargs):
        """
        Parameters
        ----------
        **launch_kwargs
            Keyword arguments to be passed to
            `playwright.chromium.launch`. For example, you can pass
            ``headless=False, slow_mo=50`` for a visualization of the
            search.
        """
        super().__init__(pw_instance, robots_txt_url, **launch_kwargs)


    def _build_state_url(self, row: NamedTuple) -> str:
        base_url = self.legal_website_dict["municode"]["base_url"]
        self.source = self.legal_website_dict["american_legal"]["source"]
        path = row.state_code.lower()
        url = f"{base_url}/{path}"
        return url

    async def _start_scrape(self, url: str):
        await self.open_webpage(url)


    async def scrape(self, locations_df: pd.DataFrame, db: MySqlDatabase) -> pd.DataFrame:

        for df in locations_df.groupby("gnis"):
            for row in df.itertuples():
                scrape_url_list = []
                scrape_url = self._build_state_url(row)
                scrape_url_list.append(scrape_url)

            for url in scrape_url_list:
                await self._start_scrape(url)


async def get_locations(db: MySqlDatabase) -> pd.DataFrame:
    command = sql_commands["get_locations"]
    return await db.async_query_to_dataframe(command)

# from utils.manual.scrape_legal_websites.insert_into_sources import import_into_sources

async def insert_into_sources(output_df: pd.DataFrame, db: MySqlDatabase) -> None:
    """
    Insert the output of main() into the MySQL database.
    """
    column_names = output_df.columns.to_list()
    insert_list = [
        row for row in output_df.itertuples(index=False, named=None)
    ]
    db.async_insert_by_batch(insert_list, columns=column_names, table="sources", update=True)
    return


async def scrape_site(df: pd.DataFrame, site_df_list: list, scraper: Scraper, db: MySqlDatabase, headless: bool=True, slowmo=1) -> pd.DataFrame:

    scraper_name =scraper.__qualname__
    if scraper_name == "MunicodeScraper":
        robots_txt_url = LEGAL_WEBSITE_DICT["municode"]["robots_txt"]
    elif scraper_name == "AmericanLegalScraper":
        robots_txt_url = LEGAL_WEBSITE_DICT["american_legal"]["robots_txt"]
    elif scraper_name == "GeneralCodeScraper":
        robots_txt_url = LEGAL_WEBSITE_DICT["general_code"]["robots_txt"]
    else:
        raise NotImplementedError("Other legal website scrapers have not been implemented.")

    try:
        async with async_playwright() as pw_instance:
            logger.info(f"Playwright instance for '{scraper_name}' instantiated successfully")
            async with await scraper(pw_instance, robots_txt_url, headless=headless, slowmo=slowmo) as scraper:
                results = await scraper.scrape(df, db)
    finally:
        logger.info(f"scraper {scraper_name} has completed its scrape. It returned {len(results)} URLs. Adding to site_df_list list...")
        site_df_list.append(results)


async def scrape_legal_websites(db: MySqlDatabase, site_df_list: list[None], scraper_list: list[Scraper], headless: bool=True, slowmo: int=100):
    outer_task_name = asyncio.current_task().get_name()
    scrapes = [
        asyncio.create_task(
            scraper.scrape_site(db, site_df_list, headless=headless, slowmo=slowmo),
            outer_task_name
        ) for scraper in scraper_list
    ]
    await asyncio.gather(*scrapes)
    return site_df_list


from config import OUTPUT_FOLDER


async def main():

    # Step 1. Instantiate the website-specific scraping classes.
    scraper_list = [
        MunicodeScraper,
        AmericanLegalScraper,
        GeneralCodeScraper,
    ]
    site_df_list =[]

    next_step(step=2, stop=True)
    # Step 2. Use selenium to scrape the websites for its URLs
    async with MySqlDatabase() as db:
        site_df_list = await scrape_legal_websites(db, site_df_list, scraper_list, headless=HEADLESS, slowmo=SLOWMO)

        next_step(step=3, stop=True)
    # Step 3. Merge the 3 dataframes into 1.
        output_df: pd.DataFrame = site_df_list.pop(0)
        for df in site_df_list[1:]:
            output_df = output_df.merge(df, on="gnis", how="left")

    next_step(step=4, stop=True)
    # Step 4. Output the URLs to a csv.
    output_df.to_csv(OUTPUT_FOLDER)
    logger.info(f"{__file__} program completed successfully! Yay!")

    sys.exit(0)


if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = base_name if base_name != "main.py" else os.path.dirname(__file__)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{program_name} program stopped.")

