
import asyncio
import os
import re
import traceback
from typing import Any, AsyncGenerator, Never

import abc
from abc import ABC, abstractmethod

import pandas as pd
from playwright.async_api import (
    Playwright as AsyncPlaywright,
    Browser as AsyncPlaywrightBrowser,
    Page as AsyncPlaywrightPage,
    BrowserContext as AsyncPlaywrightBrowserContext,
    TimeoutError as AsyncPlaywrightTimeoutError,
)

from utils.manual.scrape_legal_websites_utils.extract_urls_using_javascript import async_extract_urls_using_javascript
from utils.manual.scrape_legal_websites_utils.fetch_robots_txt import async_fetch_robots_txt
from utils.manual.scrape_legal_websites_utils.parse_robots_txt import parse_robots_txt 
from utils.manual.scrape_legal_websites_utils.can_fetch import can_fetch

from config import LEGAL_WEBSITE_DICT, CONCURRENCY_LIMIT, OUTPUT_FOLDER

from database import MySqlDatabase

from logger import Logger
logger = Logger(logger_name=__name__)


class AsyncScraper:

    def __init__(self,
                 pw_instance: AsyncPlaywright,
                 robots_txt_url: str=None,
                 user_agent: str="*",
                 **launch_kwargs):
        # Parent Class Parameters
        self.pw_instance: AsyncPlaywright = pw_instance
        self.robots_txt_url: str = robots_txt_url
        self.user_agent: str = user_agent
        self.launch_kwargs = launch_kwargs

        # Attributes initialized from functions
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)  # Currently set to 2.
        self.browser: AsyncPlaywrightBrowser = None
        self.robot_rules: dict[str, dict[str, Any]] = {}

        # Child Class attributes
        self.site_dict: dict = None
        self.scrape_url_length: int = None
        # Site-dictionary attributes.
        self.source: str = self.site_dict['source']
        self.base_url: str = self.site_dict['base_url']
        self.target_class: str = self.site_dict['target_class']
        self.robots_txt_url: str = robots_txt_url or self.site_dict['robots_txt']


    def type_check_site_dict(self, child_class_name: str, robots_txt_url: str=None) -> None:
        """
        Check if we have a site dictionary for the child class.
        """
        if self.site_dict:
            if not self.site_dict['robots_txt']:
                if not robots_txt_url:
                    logger.error(f"LEGAL_WEBSITE_DICT robots.txt entry missing for {child_class_name}.")
                    raise ValueError(f"LEGAL_WEBSITE_DICT robots.txt entry missing for {child_class_name}.")
                else:
                    logger.warning(f"LEGAL_WEBSITE_DICT robots.txt entry missing for {child_class_name}. Defaulting to input robots_txt_url...")
        else:
            raise ValueError(f"LEGAL_WEBSITE_DICT entry missing for {child_class_name}..")


    #### START CLASS STARTUP AND EXIT METHODS ####
    async def async_get_robot_rules(self, robots_txt_url) -> None:
        """
        Asynchronously Get the site's robots.txt file and assign it to the robot_urls attribute
        """
        robots_txt = await async_fetch_robots_txt(robots_txt_url)
        rules: dict[str,dict[str|Any]] = parse_robots_txt(robots_txt)
        self.robot_rules = rules


    async def _async_load_browser(self) -> None:
        """
        Asynchronously launch a chromium instance and load a page
        """
        self.browser = await self.pw_instance.chromium.launch(**self.launch_kwargs)


    async def _async_close_browser(self) -> None:
        """
        Close browser instance and reset internal attributes
        """
        if self.browser:
            await self.browser.close()
            self.browser = None


    @classmethod
    async def async_start(cls, pw_instance, robots_txt_url, user_agent, **launch_kwargs) -> 'AsyncScraper':
        """
        Factory method for asynchronously starting the class.
        """
        instance = cls(pw_instance, robots_txt_url, user_agent=user_agent, **launch_kwargs)
        await instance._async_load_browser()
        await instance.async_get_robot_rules(instance.robots_txt_url)
        return instance


    async def async_close(self) -> None:
        await self._async_close_browser()


    async def __aenter__(self) -> 'AsyncScraper':
        await self._async_load_browser()
        await self.async_get_robot_rules(self.robots_txt_url)
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.async_close()

    #### END CLASS STARTUP AND EXIT METHODS ####


    #### START PAGE PROCESSING METHODS ####
    async def _async_make_page(self) -> tuple[AsyncPlaywrightPage, AsyncPlaywrightBrowserContext]:
        """
        Make an AsyncPlaywrightPage within a browser context.
        """
        context = await self.browser.new_context()
        page = await context.new_page()
        return page, context


    async def _async_open_page(self, url: str, page: AsyncPlaywrightPage) -> AsyncPlaywrightPage:
        """
        Open a specified webpage and wait for any dynamic elements to load.
        """
        await page.goto(url)
        await page.wait_for_load_state("networkidle") # NOTE Defaults to 30 seconds wait time.
        return page


    async def _async_fetch_urls_from_page(self, url: str) -> list[dict[str,str]] | list[dict[Never]]:
        """
        Fetch URLs and their text label from a webpage.

        Example Return:
        [
          {
            href: "https://example.com/page1",
            text: "Example Link 1"
          },
          {
            href: "https://example.com/page2",
            text: "Example Link 2"
          }
        ]
        """
        page, context = await self._async_make_page()
        # Default return is an emtpy dictionary
        url_dict_list = [{"href":None, "text": None}]
        try:
            page = await self._async_open_page(url, page)
            url_dict_list = await async_extract_urls_using_javascript(page, self.source)
        except AsyncPlaywrightTimeoutError as e:
            logger.info(f"url '{url}' timed out. Returning empty dict list...")
            logger.debug(e)
        except Exception as e:
            logger.info(f"url '{url}' caused an unexpected exception: {e} ")
            traceback.print_exc()
        finally:
            await page.close()
            await context.close()
        return url_dict_list


    async def _async_respectful_fetch(self, url: str) -> list[dict[str,str]] | list[dict[Never]]:
        """
        Limit scraping a URL based on a semaphore and the delay specified in robots.txt
        """
        async with self.semaphore:
            # See if we're allowed to get the URL, as well get the specified delay from robots.txt
            fetch, delay = can_fetch(url, self.robot_rules)
            if not fetch:
                logger.warning(f"Cannot scrape URL '{url}' as it's disallowed in robots.txt")
                return [{"href":None, "text": None}]
            else: # Scrape the URL with the specified delay
                await asyncio.sleep(delay)
                return await self._async_fetch_urls_from_page(url)


    async def async_scrape(self, url: str) -> list[dict[str,str]] | None:
        """
        Scrape a URL asynchronously. Essentially a wrapper for _async_respectful_fetch.
        """
        return await self._async_respectful_fetch(url)


    def _build_url(self, state_code: str) -> str:
        """
        Implicit abstract method for building website specific URLs.
        """
        pass


    def check_url_length(self, scrape_url: str):
        """
        Type check the length of scrape url for _build_url.
        """
        if len(scrape_url) == self.scrape_url_length:
            logger.warning(f"scrape_url is not {self.scrape_url_length} characters, but {len(scrape_url)}")
            logger.debug(f"url : {scrape_url}")
            traceback.print_exc()
            raise ValueError(f"scrape_url is not {self.scrape_url_length} characters, but {len(scrape_url)}")
        else:
            logger.debug(f"{self.source} scrape_url built successfully.")
            return scrape_url


    def build_urls(self, locations_df: pd.DataFrame) -> list[dict[str,str]]:
        """
        Create scrape URLs from the locations dataframe.
        """
        # Drop duplicate states.
        state_codes_df = locations_df.drop_duplicates(subset=['state_code'])

        #Iterate through the states and build URLs out of them.
        state_url_dict_list = [
            {"state_code": row.state_code, "state_url": self._build_url(row.state_code)}
            for row in state_codes_df.itertuples()
        ]
        logger.info("Created state_code URLs for General Code",f=True)
        return state_url_dict_list


    def _save_output_df_to_csv(self, dic: dict) -> dict:
        """
        Save each URL's results as a separate CSV file.
        This is so we don't want to lose data if we're kicked off the site.

        Args:
            dic (dict): A dictionary of state_code, state_url, href, and text. 
                - NOTE href and text are under the key 'result' in the input
        Returns:
            The dict that went into the function.
        Raises:
            Exception: Anytime there is an unexpected error normalizing dic or saving the normalized dic to CSV
        """
        # Initialize variables.
        state_code, state_url, result = tuple(dic.keys())
        output_filename = f"{state_code}_{self.source}.csv"
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)

        if result:
            logger.info(f"Got {len(result)} results for URL {state_url}. ")
            logger.debug(f"dic for {state_url}\n{dic}", f=True)

            try:
                # Make the output_df by normalizing the input dictionary.
                output_df: pd.DataFrame = pd.json_normalize(dic, "result", ["state_code", "state_url", "href", "text"])
                logger.debug(f"output_df\n{output_df.head()}",f=True)
                # Save it to a CSV file.
                output_df.to_csv(output_path)
                logger.info(f"{output_filename} saved to output folder successfully.")
            except Exception as e:
                logger.exception(f"Exception while saving {output_filename} to output folder: {e}")
            finally:
                return dic
        else:
            logger.info(f"No results returned for {state_url}.")
            return dic


    async def _filter_urls(self, urls: list[dict[str,str]], db: MySqlDatabase) -> list[dict[str,str]]:
        """
        Filter out URLs we've already scraped based on whether we've got a csv of them already or whether they're in the database.
        Args:
            urls:
        Return:
        """
        command = """
        SELECT source_municode, source_general_code, source_american_legal, source_code_publishing_co, source_place_domain
        FROM sources
        """
        unprocessed_urls = []
        processed_urls = await db.query_to_dataframe(command)
        for url in urls:
            output_filename = f"{url['state_code']}_{self.source}.csv"
            output_path = os.path.join(OUTPUT_FOLDER, output_filename)
            if not os.path.isfile(output_path):
                unprocessed_urls.append(url)
        return unprocessed_urls


    async def scrape(self, locations_df: pd.DataFrame, urls: list[dict[str,str]], db: MySqlDatabase) -> pd.DataFrame:
        """
        Scrape URLs and add it to a dataframe.
        Args:
            locations_df: A Dataframe of data from the table 'locations' in the database.
            urls: a list of URLs to scrape.
            db: a MySQL database instance
        Returns:
            A dataframe
        Examples:
        >>> # Example Return
        >>>     state_code                           state_url                                       href               text
        >>> 0         "AK"  "https://library.municode.com/ak/"  "https://library.municode.com/ak/ashvile"  "City of Ashvile"
        >>> 1         "AL"  "https://library.municode.com/al/"  "https://library.municode.com/al/johnson"          "Johnson"
        >>> 2         "AR"  "https://library.municode.com/ar/"  "https://library.municode.com/ar/vikberg"   "Vikberg County"
        """
        results_url_dict_list = []
        unprocessed_urls = await self._filter_urls(urls, db)

        # Scrape the URL and return its URLs and text.
        logger.info(f"Getting URLs from {self.source}...")
        # Create generator that creates a list of dictionaries for each item return of async_scrape.
        dict_generator: AsyncGenerator = (
            {
                'state_code': url['state_code'],
                'state_url': url['state_url'],
                'result': result # NOTE We don't unpack the dictionary since json_normalize will do that for us.
            } for url in unprocessed_urls for result in await self.async_scrape(url['state_code'])
        )
        results_url_dict_list = [ # Normalize the dictionaries, save them as CSVs, and append them to results_url_dict_list
            self._save_output_df_to_csv(dic) async for dic in dict_generator
        ] # -> list[dict]

        # Merge the created URLs with locations dataframe.
            # NOTE Merge works instead of join because merge works on strings instead of indexes.
            # See: https://stackoverflow.com/questions/50649853/trying-to-merge-2-dataframes-but-get-valueerror
        output_df = pd.DataFrame.from_dict(results_url_dict_list)
        logger.debug(f"output_df\n{output_df}",f=True)
        site_df = locations_df.merge(results_url_dict_list, on="state_code", how="inner")
        logger.debug(f"site_df\n{site_df}",f=True)

        # Get rid of duplicate URLs
        site_df = site_df.drop_duplicates(subset=['href'])

        # Append the source.
        site_df['source'] = self.source

        return site_df




