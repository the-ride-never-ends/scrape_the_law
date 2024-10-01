

import asyncio
from contextlib import AsyncExitStack
import os
import sys
import time
from typing import NamedTuple


import pandas as pd
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

# Insert the top-level directory as a filepath to prevent import errors. We'll see if it works.
insert_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0,insert_path)

from database import MySqlDatabase

from config import OUTPUT_FOLDER, INSERT_BATCH_SIZE, LEGAL_WEBSITE_DICT, CONCURRENCY_LIMIT, HEADLESS, SLOW_MO

from logger import Logger
logger = Logger(logger_name=__name__)

# TODO Figure out what the hell is up with these imports.
from utils.shared.next_step import next_step
from utils.manual.scrape_legal_websites_utils.get_robots_txt_url import get_robots_txt_url

from manual.scraper_base_class.async_scraper import AsyncScraper
from manual.scraper_base_class.scraper import Scraper


class GeneralCodeScraper(Scraper, AsyncScraper):

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


    def _build_state_url(self, state_code: str) -> str:
        self.source = self.legal_website_dict["general_code"]["source"]
        base_url = self.legal_website_dict["general_code"]["base_url"]

        url = f"{base_url}{state_code}" # -> "https://www.generalcode.com/source-library/?state=AZ"

        #logger.debug(f"url : {url}")
        # NOTE The URL must be 52 characters long.
        assert len(url) == 52, f"scrape_url is not 52 characters, but {len(url)}"
        return url


    async def scrape(self, locations_df: pd.DataFrame, db: MySqlDatabase) -> pd.DataFrame:
        """
        Create the scrape URLs and add it to a dataframe
        """
        # Create the URLs
        state_url_dict_list = []
        for state_code, df in locations_df.groupby('state_code'):
            logger.debug(f"Creating URLs for state_code '{state_code}'...")
            state_url = self._build_state_url(state_code)
            logger.debug("state_url built successfully.")
            state_url_dict_list.append(
                {"state_code": state_code, "state_url": state_url}
            )
        logger.info("Created state_code URLs for General Code",f=True)

        # Merge the created URLs with locations dataframe.
        # NOTE Merge works instead of join because merge works on strings instead of indexes.
        #  See: https://stackoverflow.com/questions/50649853/trying-to-merge-2-dataframes-but-get-valueerror
        #state_urls_df = pd.DataFrame.from_records(state_url_list, columns=["state_code", "state_url"])
        #locations_df = locations_df.merge(state_urls_df, on="state_code", how="inner")

        # locations_df.join(state_urls_df, on="state_code", how="inner")
        #logger.debug(f"locations_df\n{locations_df.head()}",f=True)

        # NOTE Pandas doesn't support async for-lists, so we need to do everything after the fact.
        async for dict in state_url_dict_list:
            logger.info(f"Getting URLs for '{dict['state_code']}'")




        for state_code, df in locations_df.groupby('state_code'):
            # Convert DataFrame to a list of tuples
            df_tuples = list(df.itertuples(index=False, name=None))
            logger.info(f"Getting URLs for '{state_code}'")
            logger.debug(f"df {state_code}: {df_tuples[:5]}")  # Show first 5 tuples
            time.sleep(10)
            await self.async_scrape(state_url) # -> dict[str,str] | dict[None]
            state_url_list.append(state_url)



class AmericanLegalScraper(Scraper, AsyncScraper):

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
        self.source = self.legal_website_dict["american_legal"]["source"]
        base_url = self.legal_website_dict["american_legal"]["base_url"]

        path = row.state_code.lower()
        url = f"{base_url}{path}" # -> "https://codelibrary.amlegal.com/regions/az"

        logger.debug(f"url : {url}")
        assert len(url) == 42, f"scrape_url is not 40 characters, but {len(url)}"
        return url


class MunicodeScraper(Scraper, AsyncScraper):
    """
    Search for top results on google and return their links.\n
    NOTE This has been heavily modified from ELM's original code. We'll see if it's more effective in the long run.

    Parameters:
        pw_instance: An asynchronous or synchronous playwright instance.
        robots_txt_url: The URL for a 
        **launch_kwargs:
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


    def _build_state_url(self, state_code: str) -> str:
        """
        Create the URL paths for the domain we want to scrape.
        """
        self.source = self.legal_website_dict["american_legal"]["source"]
        base_url = self.legal_website_dict["municode"]["base_url"]

        path = state_code.state_code.lower()
        state_url = f"{base_url}/{path}" # -> https://library.municode.com/az

        logger.debug(f"state_url: {state_url}")
        assert len(state_url) == 31, f"scrape_url is not 40 characters, but {len(state_url)}"
        return state_url


    def _check_against_href_text():
        """"
        Check to see if the place name is in the href's text from the website
        """
        pass


    async def scrape(self, locations_df: pd.DataFrame, db: MySqlDatabase) -> pd.DataFrame:
        """
        Create the scrape URLs and add it to a dataframe
        """

        state_url_list = []
        for state_code, df in locations_df.groupby('state_code'):
            logger.info(f"Creating URLs for state_code '{state_code}'...")
            state_url = self._build_state_url(state_code)
            logger.info("state_url built successfully.")
            state_url_list.append((state_code, state_url,))

        state_urls_df = pd.DataFrame.from_records(state_url_list, columns=["state_code", "state_url"])
        locations_df.join(state_urls_df, on="state_code", how="inner")
        logger.debug(locations_df.head())

        for idx, row in enumerate(locations_df.groupby('state_code'), start=1):
            logger.info(f"Getting ULRs under {row.state_url}")
            logger.debug(f"row {idx}: {row}")
            await self.async_scrape(state_url) # -> dict[str,str] | dict[None]
            state_url_list.append(state_url)


async def get_locations(db: MySqlDatabase) -> pd.DataFrame:
    command = """
        SELECT gnis, place_name, class_code, state_code FROM locations;
        """
    locations_df: pd.DataFrame = await db.async_query_to_dataframe(command)
    return locations_df


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




async def scrape_site(db: MySqlDatabase, scraper: Scraper, site_df_list: list, locations_df: pd.DataFrame=None, headless: bool=True, slow_mo: int=1) -> None:
    # Get the robots.txt file
    scraper_name =scraper.__qualname__
    robots_txt_url = get_robots_txt_url(scraper_name)
    logger.debug(f"robots_txt_url for {scraper}: {robots_txt_url}")

    # Create an exit stack.
    async with AsyncExitStack() as stack:
        # Create a playwright instance within the stack.
        pw_instance = await stack.enter_async_context(async_playwright())
        logger.info(f"Playwright instance for '{scraper_name}' instantiated successfully")

        # Instantiate each scraper.
        scraper_instance: Scraper = await stack.enter_async_context(
            scraper(pw_instance, robots_txt_url, headless=headless, slow_mo=slow_mo)
        )

        # Scrape each domain and return the results.
        results = await scraper_instance.scrape(locations_df, db)

    logger.info(f"scraper {scraper_name} has completed its scrape. It returned {len(results)} URLs. Adding to site_df_list list...")
    site_df_list.append(results)


async def scrape_legal_websites(db: MySqlDatabase,
                                site_df_list: list[None],
                                scraper_list: list[Scraper],
                                locations_df: pd.DataFrame=None,
                                headless: bool=True,
                                slow_mo: int=100
                                ) -> list[pd.DataFrame]:
    """
    Scrape legal websites concurrently using site-specific Scraper classes.

    Args:
        db (MySqlDatabase): The database connection object.
        site_df_list (list[None]): An empty list that will be populated with DataFrames containing the scraped data from each site.
        scraper_list (list[Scraper]): A list of Scraper classes, each representing a different legal website to be scraped.
        headless (bool, optional): Whether to run the browser in headless mode. Defaults to True.
        slow_mo (int, optional): The number of milliseconds to wait between actions. Defaults to 100.

    Returns:
        list[pd.DataFrame]: A list of pandas DataFrames, each containing the scraped data from a legal website.
    """
    outer_task_name = asyncio.current_task().get_name()
    scrapes = [
        asyncio.create_task(
            scrape_site(db, scraper, site_df_list, locations_df=locations_df, headless=headless, slow_mo=slow_mo),
            name=f"{outer_task_name}_{scraper.__qualname__}"
        ) for scraper in scraper_list
    ]
    await asyncio.gather(*scrapes) # -> list[dict]
    return site_df_list



async def main():

    # Step 1. Define the website-specific scraping classes and output list.
    scraper_list = [
        #MunicodeScraper,
        #AmericanLegalScraper,
        GeneralCodeScraper,
    ]
    site_df_list = []


    next_step(step=2, stop=False)
    # Step 2. Use Playwright to scrape the websites for its URLs
    async with MySqlDatabase(database="socialtoolkit") as db:

        # Get the locations dataframe
        locations_df = await get_locations(db)
        logger.debug(f"locations_df: {locations_df.head()}\ndtypes: {locations_df.dtypes}")

        site_df_list = await scrape_legal_websites(db, site_df_list, scraper_list, 
                                                   locations_df=locations_df, headless=HEADLESS, slow_mo=SLOW_MO)


        next_step(step=3, stop=True)
    # Step 3. Merge the 3 dataframes into 1.
        output_df: pd.DataFrame = site_df_list.pop(0)
        for df in site_df_list[1:]:
            output_df = output_df.merge(df, on="gnis", how="left")


    next_step(step=4, stop=True)
    # Step 4. Output the URLs to a csv.
    output_df.to_csv(OUTPUT_FOLDER)


    next_step(step=5, stop=True)
    # Step 5. Insert the output_df into the database


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

