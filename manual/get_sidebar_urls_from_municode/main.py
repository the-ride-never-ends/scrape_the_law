import asyncio
import shutil
import os

from typing import Callable


import pandas as pd

# Get the top level directory and put it in the path.
# Otherwise, we get a ModuleNotFoundError
from pathlib import Path
import sys
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from database import MySqlDatabase
from config import CUSTOM_MODULES_FOLDER, OUTPUT_FOLDER, LEGAL_WEBSITE_DICT
from logger import Logger
logger = Logger(logger_name=__name__)

from utils.shared.next_step import next_step
from utils.shared.get_urls_with_selenium import get_sidebar_urls_from_municode_with_selenium
from scraper.sites.municode.library.ScrapeMunicodePage import get_sidebar_urls_from_municode_with_playwright

from utils.shared.make_sha256_hash import make_sha256_hash

from utils.database.get_column_names import get_column_names
from utils.database.get_num_placeholders import get_num_placeholders


def copy_file_to_current_folder(source_folder, filename):
    source_path = os.path.join(source_folder, filename)
    destination_path = os.path.join(os.getcwd(), filename)
    shutil.copy2(source_path, destination_path)
    print(f"File '{filename}' copied to the current folder.")


async def get_urls_with_playwright(df: pd.DataFrame, steps: list[Callable] = None):

    wait_in_seconds = LEGAL_WEBSITE_DICT["municode"]['wait_in_seconds']
    sleep_length = 1
    results = []
    for row in df.itertuples():
        result = row.url
        for func in steps:
            result = func(result, wait_in_seconds)
        results.append(results)

sidebar_list = {
    "id": {
        "sidebar": {
            "id": "genToc_", # Regex this.
            "data-ng-bind": "::node.Heading"
        },
        "menu_bar": {
            "additional_city_urls": {
                "a_xpath": "/html/body/div[1]/div[1]/ui-view/mcc-client-menu/nav/ul/li[6]/ul/li/a" #href Needs click to access
            }
        }
    } # TODO regex this.
}

# async def get_municode_sidebar_urls(sources_df: pd.DataFrame, driver: webdriver.Chrome, results: list,) -> pd.DataFrame: 

#     wait_in_seconds = LEGAL_WEBSITE_DICT["municode"]['wait_in_seconds']
#     sleep_length = 5
#     sidebar_class = None #LEGAL_WEBSITE_DICT["municode"]['target_class']
#     results = []
#     for row in sources_df.itertuples:
#         try:
#             result = await get_urls_with_selenium(row.url, 
#                                                   driver, 
#                                                   wait_in_seconds, 
#                                                   sleep_length=sleep_length,
#                                                   class_=sidebar_class)
#             results.append(result)
#         except WebDriverException as e:
#             logger.error(f"Selenium error retrieving {row.url}: {e}")
#     output_df = pd.DataFrame.from_records(results)

wait_in_seconds = LEGAL_WEBSITE_DICT['municode']['wait_in_seconds']
domain = "https://municode.com/"

DEBUG = True

async def main():


    next_step("Step 1. Get municode URLs from database")
    async with MySqlDatabase(database="socialtoolkit") as db:
        # Get source_df and url_hashes_df from the database
        sources_df = await db.async_query_to_dataframe(
            """
            SELECT source_municode AS url, gnis, place_name 
                FROM sources 
                WHERE source_municode IS NOT NULL;"""
        )
        logger.info(f"sources_df\n{sources_df.head()}",f=True)
        url_hashes_df = await db.async_query_to_dataframe(
            """
            SELECT DISTINCT url_hash FROM urls 
            WHERE url LIKE '%municode%';
            """
        )
        logger.debug(f"url_hashes_df\n{url_hashes_df.head()}",f=True)

        # Filter out URLs that are already in table urls.
        len_begin = len(sources_df)
        sources_df['url_hash'] = sources_df.apply(lambda row: make_sha256_hash(row['gnis'], row['url']), axis=1)
        sources_df: pd.DataFrame = sources_df[~sources_df['url_hash'].isin(url_hashes_df['url_hash'])]
        logger.info(f"Filtered out {len_begin - len(sources_df)} Municode URLs from 'sources' that are already in table 'urls'")
        logger.info(f"sources_df\n{sources_df.head()}",f=True)


        next_step("Step 2. Scrape the Municode URLs for table of contents links and code versions")
        if DEBUG:
            logger.debug("DEBUG mode. Only getting the first 5 rows")
            sources_df = sources_df.head(5)
            logger.debug(f"sources_df\n{sources_df.head()}")


        urls_df: pd.DataFrame = await get_sidebar_urls_from_municode_with_playwright(sources_df)
        logger.info(f"urls_df\n{urls_df.head()}",f=True)


        next_step("Step 3. Insert the URLs into the database.")
        args = {
            "columns": get_column_names(urls_df),
            "placeholders": get_num_placeholders(len(urls_df)),
        }
        await db.async_dataframe_to_insert(
            """
            INSERT INTO urls ({columns}) VALUES ({placeholders}) 
            """,
            df=urls_df,
            args=args
        )


if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = base_name if base_name != "main.py" else os.path.basename(os.getcwd())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{program_name} program stopped.")

