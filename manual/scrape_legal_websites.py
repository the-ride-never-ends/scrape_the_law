

import asyncio
from contextlib import AsyncExitStack
import os
import sys
import time
from typing import NamedTuple, Never, Self, Generator, AsyncGenerator

import abc

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

from config import OUTPUT_FOLDER, LEGAL_WEBSITE_DICT, HEADLESS, SLOW_MO

from logger import Logger
logger = Logger(logger_name=__name__)

# TODO Figure out what the hell is up with these imports.
from utils.shared.next_step import next_step
from utils.manual.scrape_legal_websites_utils.get_robots_txt_url import get_robots_txt_url
from utils.manual.scrape_legal_websites_utils.get_locations import get_locations
from utils.manual.scrape_legal_websites_utils.match_urls_to_locations import match_urls_to_locations


from manual.scraper_base_class.AsyncScraper import AsyncScraper

# state_urls_df = pd.DataFrame.from_records(state_url_list, columns=["state_code", "state_url"])
# locations_df = locations_df.merge(state_urls_df, on="state_code", how="inner")

# locations_df.join(state_urls_df, on="state_code", how="inner")
# logger.debug(f"locations_df\n{locations_df.head()}",f=True)


LEGAL_WEBSITE_DICT = {
    "american_legal": {
        "base_url": "https://codelibrary.amlegal.com/regions/",
        "target_class": "browse-link roboto",
        "wait_in_seconds": 5,
        "robots_txt": "https://codelibrary.amlegal.com/robots.txt",
        "source": "american_legal",
    },
    "municode": {
        "base_url": "https://library.municode.com/",
        "target_class": "index-link",
        "wait_in_seconds": 15,
        "robots_txt": "https://municode.com/robots.txt",
        "source": "municode",
    },
    "general_code" : {
        "base_url": "https://www.generalcode.com/source-library/?state=",
        "target_class": "codeLink",
        "wait_in_seconds": 0,
        "robots_txt": "https://www.generalcode.com/robots.txt",
        "source": "general_code",
    },
}



class GeneralCodeScraper(AsyncScraper):
    """
    Parameters:
        pw_instance (AsyncPlaywright): An Async Playwright Instance
        robots_txt_url (str): A URL for a site's robots.txt. Defaults to self.site_dict['robots_txt']
        **launch_kwargs: Keyword arguments to be passed to `playwright.chromium.launch`.
    """

    def __init__(self, pw_instance, robots_txt_url:str=None, **launch_kwargs):
        # Initialize the child class attributes
        self.scrape_url_length = 52
        self.site_dict = LEGAL_WEBSITE_DICT['general_code_co'] or None
        self.type_check_site_dict(self, __class__.__qualname__, robots_txt_url=robots_txt_url)

        # Initialize the parent class attributes
        super().__init__(pw_instance, robots_txt_url=robots_txt_url, **launch_kwargs)


    def _build_url(self, state_code: str) -> str:
        """
        Build General Code Co-specific URLs.
        NOTE: The URL must be 52 characters long.
        Example Output:
            https://www.generalcode.com/source-library/?state=AZ
        """
        # Get URL components from legal_website_dict
        logger.debug(f"Creating URLs for state_code '{state_code}'...")
        scrape_url = f"{self.base_url}{state_code}" # -> "https://www.generalcode.com/source-library/?state=AZ"
        scrape_url = self.check_url_length(scrape_url)
        return scrape_url


class AmericanLegalScraper(AsyncScraper):
    """
    Parameters:
        pw_instance (AsyncPlaywright): An Async Playwright Instance
        robots_txt_url (str): A URL for a site's robots.txt. Defaults to self.site_dict['robots_txt']
        **launch_kwargs: Keyword arguments to be passed to `playwright.chromium.launch`.
    """

    def __init__(self, pw_instance, robots_txt_url:str=None, **launch_kwargs):
        # Initialize the child class attributes
        self.scrape_url_length = 42
        self.site_dict = LEGAL_WEBSITE_DICT['american_legal'] or None
        self.type_check_site_dict(self, __class__.__qualname__, robots_txt_url=robots_txt_url)

        # Initialize the parent class attributes
        super().__init__(pw_instance, robots_txt_url, **launch_kwargs)


    def _build_url(self, state_code: str) -> str:
        """
        Build  American Legal-specific URLs.
        NOTE: The URL must be 52 characters long.
        Example Output:
            https://www.generalcode.com/source-library/?state=AZ
        """
        scrape_url = f"{self.base_url}{state_code.lower()}" # -> "https://codelibrary.amlegal.com/regions/az"

        logger.debug(f"Creating URLs for state_code '{state_code}'...")
        scrape_url = self.check_url_length(scrape_url)
        return scrape_url


class MunicodeScraper(AsyncScraper):
    """
    Parameters:
        pw_instance (AsyncPlaywright): An Async Playwright Instance
        robots_txt_url (str): A URL for a site's robots.txt. Defaults to self.site_dict['robots_txt']
        **launch_kwargs: Keyword arguments to be passed to `playwright.chromium.launch`.
    """

    def __init__(self, pw_instance, robots_txt_url:str=None, **launch_kwargs):
        # Initialize the child class attributes
        self.scrape_url_length = 31
        self.site_dict = LEGAL_WEBSITE_DICT['municode'] or None
        self.type_check_site_dict(self, __class__.__qualname__, robots_txt_url=robots_txt_url)

        # Initialize the parent class attributes
        super().__init__(pw_instance, robots_txt_url=robots_txt_url, **launch_kwargs)


    def _build_url(self, state_code: str) -> str:
        """
        Build  Municode-specific URLs.
        NOTE: The URL must be 32 characters long.
        Example Output:
            https://library.municode.com/az
        """
        scrape_url = f"{self.base_url}/{state_code.lower()}" # -> https://library.municode.com/az

        logger.debug(f"Creating URLs for state_code '{state_code}'...")
        scrape_url = self.check_url_length(scrape_url)
        return scrape_url


async def insert_into_sources(output_df: pd.DataFrame, db: MySqlDatabase) -> None:
    """
    Insert the output of main() into the 'sources' table in the MySQL database.\n
    Insert command is
    ```INSERT INTO sources (gnis, source_municode, source_general_code, source_american_legal, 
                            source_code_publishing_co, source_place_domain) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        gnis = VALUES(gnis),
        source_municode = VALUES(source_municode),
        source_general_code = VALUES(source_general_code),
        source_american_legal = VALUES(source_american_legal),
        source_code_publishing_co = VALUES(source_code_publishing_co),
        source_place_domain = VALUES(source_place_domain);
    ```
    """
    column_names = output_df.columns.to_list()
    insert_list = [
        row for row in output_df.itertuples(index=False, named=None)
    ]
    db.async_insert_by_batch(insert_list, columns=column_names, table="sources", update=True)
    return


async def scrape_site(db: MySqlDatabase, 
                      scraper: AsyncScraper, 
                      site_df_list: list, 
                      locations_df: pd.DataFrame=None, 
                      headless: bool=True, 
                      slow_mo: int=1
                     ) -> None:
    """
    Scrape a website using Playwright
    """
    # Get the robots.txt file
    scraper_name =scraper.__qualname__
    logger.debug(f"robots_txt_url for {scraper}: {get_robots_txt_url(scraper_name)}")

    # Create an exit stack.
    async with AsyncExitStack() as stack:
        # Create a playwright instance within the stack.
        pw_instance = await stack.enter_async_context(async_playwright())
        logger.info(f"Playwright instance for {scraper_name} instantiated successfully")

        # Instantiate each scraper.
        scraper_instance: AsyncScraper = await stack.enter_async_context(
            scraper(pw_instance, robots_txt_url=get_robots_txt_url(scraper_name), headless=headless, slow_mo=slow_mo)
        )

        # Build URL paths for each domain.
        urls: list[dict[str,str]] = await scraper_instance.build_urls(locations_df)

        # Scrape each domain and return the URLs and their associated text.
        sites_df: pd.DataFrame = await scraper_instance.scrape(locations_df, urls, db)

        # Match the URLs and text with the locations in locations_df.
        results: pd.DataFrame  = match_urls_to_locations(locations_df, sites_df)

    logger.info(f"scraper {scraper_name} has completed its scrape. It returned {len(results)} URLs. Adding to site_df_list list...")
    site_df_list.append(results)


async def scrape_legal_websites(db: MySqlDatabase,
                                site_df_list: list[None],
                                scraper_list: list[AsyncScraper],
                                locations_df: pd.DataFrame=None,
                                headless: bool=True,
                                slow_mo: int=100
                                ) -> list[pd.DataFrame]:
    """
    Scrape legal websites concurrently using site-specific AsyncScraper classes.

    Args:
        db (MySqlDatabase): The database connection object.
        site_df_list (list[None]): An empty list that will be populated with DataFrames containing the scraped data from each site.
        scraper_list (list[AsyncScraper]): A list of AsyncScraper classes, each representing a different legal website to be scraped.
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
    # Step 5. Make the boolean URLs df


    next_step(step=6, stop=True)
    # Step 6. Insert the output_df into the database


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

