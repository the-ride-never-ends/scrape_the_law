
import asyncio
from collections import deque
import os
import re
import time
import sys
from typing import NamedTuple, Never

import aiohttp
import pandas as pd
from playwright.async_api import (
    async_playwright,
    PlaywrightContextManager as AsyncPlaywrightContextManager,
    ElementHandle,
    expect,
    Locator,
    Error as AsyncPlaywrightError,
    TimeoutError as AsyncPlaywrightTimeoutError
)

from scraper.child_classes.playwright.AsyncPlaywrightScrapper import AsyncPlaywrightScrapper

from utils.shared.limiter_utils.Limiter import Limiter
from utils.shared.load_from_csv import load_from_csv
from utils.shared.make_sha256_hash import make_sha256_hash
from utils.shared.next_step import next_step
from utils.shared.sanitize_filename import sanitize_filename
# from utils.shared.raise_value_error_if_absent import raise_value_error_if_absent


from config import OUTPUT_FOLDER, PROJECT_ROOT
SCREENSHOT_SEMAPHORE: int = 10
CHECK_IF_URL_IS_UP_SEMAPHORE: int = 5

from logger import Logger
logger = Logger(logger_name=__name__)


SCREENSHOT_FOLDER = os.path.join(OUTPUT_FOLDER, "screenshots")
CSV_OUTPUT_FOLDER = os.path.join(OUTPUT_FOLDER, "csv")
ouput_folder_list = [SCREENSHOT_FOLDER, CSV_OUTPUT_FOLDER]
for folder in ouput_folder_list:
    if not os.path.exists(folder):
        print(f"Creating output folder: {folder}")
        os.mkdir(folder)


def raise_value_error_if_absent(*args) -> Never:
    """
    Take a list of arguments and raise a Value Error if any of them are absent.
    """
    args = [*args]
    if not all(args):
        args = " ,".join(args)
        raise ValueError(f"{args} must be provided.")


def list_of_dicts_to_csv_via_pandas(list_of_dicts: list[dict], 
                                    filename: str, 
                                    index: bool = False,
                                    logger: Logger = None, 
                                    output_path: str = None,
                                    ) -> None:
    """
    Save a list of dictionaries to a CSV file via a Pandas.
    TODO Add in other options. Pandas has a LOT of them.
    """
    # Type checking.
    if isinstance(list_of_dicts, list) and isinstance(list_of_dicts[0], dict):
        error_message = f"list_of_dicts argument is not a list of dicts, but a {type(list_of_dicts)}"
        logger.error(error_message)
        raise ValueError(error_message)
    assert logger, "No logger provided."

    # Define the output path.
    output_path = os.path.join(output_path, filename) or os.path.join(CSV_OUTPUT_FOLDER, filename)

    # Save list_of_dicts to csv.
    pd.DataFrame().from_records(list_of_dicts).to_csv(filename, index=index)
    logger.info(f"{filename} saved to {CSV_OUTPUT_FOLDER}.")

    return



async def check_if_url_is_up(row: NamedTuple, 
                            timeout: int = 10,
                            good_response_list: list=None, 
                            bad_response_list: list=None,
                            ) -> dict:
    """
    
    """

    raise_value_error_if_absent(good_response_list, bad_response_list)
    # Intialize row.url alias.
    url = row.url

    # Initialize the output dictionary
    output_dict = {
        'gnis': row.gnis,
        'url': url,
        'place_name': row.place_name,
        'response_status': None,
        'filter_out': True,  # Default to filtered out unless we confirm it's good
        'error': 'NA'
    }

    try: 
        # Get the status code from the URL.
        timeout_client = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(url, timeout=timeout_client) as session:
            async with session.get() as response:
                output_dict['response_status'] = response.status

                if response.status == 200:
                    output_dict['filter_out'] = False
                    print(f"{url} is up.")
                else:
                    logger.warning(f"{url} is down: {response.status}")
                    print("Recording then skipping...")

    # NOTE We don't raise these, since we want the 404s or any other errors to be recorded.
    except aiohttp.ClientError as e:
        mes = f"{e.__qualname__} for {url}: {e}"
    except asyncio.TimeoutError:
        mes = f"{e.__qualname__} for {url}"
    except Exception as e:
        mes = f"{e.__qualname__} for {url}: {e}"
    finally:
        if mes:
            logger.error(mes)
            output_dict['error'] = mes

    # Route the dictionary to the appropriate list.
    if output_dict['filter_out'] :
        bad_response_list.append(output_dict)
    else:
        good_response_list.append(output_dict)
    return


class GetFrontPages(AsyncPlaywrightScrapper):
    """
    Take a screenshot of a domain's front page.
    """
    def __init__(self,
                domain: str,
                pw_instance: AsyncPlaywrightContextManager,
                *args,
                user_agent: str="*",
                **kwargs):
        super().__init__(domain, pw_instance, *args, user_agent=user_agent, **kwargs)
        self.output_dir = SCREENSHOT_FOLDER

    async def get_screenshot_of_front_page(self, 
                                           row: NamedTuple, 
                                           success_list: list=None, 
                                           failure_list: list=None,
                                           ) -> None:

        raise_value_error_if_absent(success_list, failure_list)

        # Define the screenshot filename.
        prefix = "front_page_"
        screenshot_postfix = sanitize_filename(f"{row.place_name}_{row.gnis}") + ".jpeg"

        output_dict = {
            "gnis": row.gnis,
            "url": row.url,
            "place_name": row.place_name,
            "screenshot_path": None,
        }

        try:
            # Open the page.
            # NOTE This function automatically opens up a new page and context
            logger.info(f"Going to {row.url}...")
            await self.navigate_to(row.url)

            # Take a screenshot of the page and save it.
            await self.take_screenshot(
                filename=screenshot_postfix,
                prefix=prefix,
            )

            # If we're successful, save to success_list
            logger.info(f"Screenshot successful. Saved to {self.screenshot_path}")
            output_dict['screenshot_path'] = self.screenshot_path
            success_list.append(output_dict)

        except:
            # If we're not successful, save it to failure_list.
            # NOTE: Error try-except loops for Playwright Classes are always implemented at the lowest function level
            # via the try_except or async_try_except decorators, unless otherwise specified.
            logger.error("Could not take screenshot. Appending to failure_list...")
            failure_list.append(output_dict)

        finally:
            # Close the page and context
            await self.close_current_page_and_context()
            return


async def scraper_class_wrapper(row, 
                                pw_instance: AsyncPlaywrightContextManager=None, 
                                success_list: list=None, 
                                failure_list: list=None
                                ) -> None:
    """
    This is essentially a limiter-friendly context manager for the GetFrontPages class.
    """
    raise_value_error_if_absent(pw_instance, success_list, failure_list)

    scraper: GetFrontPages = await GetFrontPages(row.url, pw_instance).start(row.url, pw_instance)
    await scraper.get_screenshot_of_front_page(row, success_list, failure_list)
    await scraper.exit()
    return


async def main() -> None:
    """
    Program: Get screenshots of front pages
    """


    next_step("Step 1. Load front page URLs from the CSV.")
    frontpage_urls_dict_csv_path = os.path.join(PROJECT_ROOT, 'input', "frontpage_urls.csv")
    frontpage_urls_dict = load_from_csv(frontpage_urls_dict_csv_path)
    urls_df = pd.DataFrame().from_dict(frontpage_urls_dict)

    # If we're not working with data from the locations data, we need to create the gnis and place_name columns
    if not urls_df['place_name']:
        urls_df['place_name'] = urls_df['url'].apply(lambda x: x.split('/')[2])

    if not urls_df['gnis']:
        urls_df['gnis'] = make_sha256_hash(urls_df['url'], urls_df['place_name'] )


    next_step("Step 2. Filter out front pages where we already got screenshots", stop=True)
    processed_urls_csv_path = os.path.join(CSV_OUTPUT_FOLDER, "processed_frontpage_urls.csv")
    frontpage_urls_dict = load_from_csv(processed_urls_csv_path)
    processed_urls_df = load_from_csv(processed_urls_csv_path)



    # NOTE: The CSV has a column called "url" that contains the URLs, 
    # and 'gnis', a unique ID variable from the MySQL database.
    urls_df = urls_df[~urls_df['gnis'].isin(processed_urls_df['gnis'])]
    assert len(urls_df.index) != 0, "No URLs to process."	


    next_step("Step 3. Filter out front pages that gave bad responses.")


    logger.debug(f"urls_df.head()\n{urls_df.head()}") # NOTE: The CSV has a column called "url" that contains the URLs.
    


    next_step("Step 3. Check if the URLs are up. If they aren't, note that and filter them out.")
    # Instantiate limiter class
    limiter = Limiter(semaphore=CHECK_IF_URL_IS_UP_SEMAPHORE, progress_bar=True, )
    good_response_list = bad_response_list = []

    # Check if the URLs are up and separate them.
    await limiter.run_async_many(
        inputs=urls_df, 
        func=check_if_url_is_up,
        good_response_list=good_response_list,
        bad_response_list=bad_response_list,
    ) # -> list[dict], list[dict]

    # Save the good and bad response lists to CSVs.
    # Good responses are the URLs that are up and will be processed.
    good_urls_df = pd.DataFrame().from_records(good_response_list)
    urls_df.to_csv(
        os.path.join(CSV_OUTPUT_FOLDER, "good_response_urls.csv"),
        index=False
    )
    pd.DataFrame().from_records(bad_response_list).to_csv(
        os.path.join(CSV_OUTPUT_FOLDER, "bad_response_urls.csv"),
        index=False
    )


    next_step("Step 4. Take screenshots of the the URLs and save them.")
    success_list = failure_list = []
    with async_playwright() as pw_instance:

        # Define the limiter.s
        limiter = Limiter(semaphore=SCREENSHOT_SEMAPHORE,progress_bar=True)

        # Take screenshots of all the URLs
        await limiter.run_async_many(
            inputs=good_urls_df, 
            func=scraper_class_wrapper,
            pw_instance=pw_instance,
            success_list=success_list,
            failure_list=failure_list
        ) # -> list[dict], list[dict]


    next_step("Step 5. Save the processed URLs to CSV files.")
    pd.DataFrame().from_records(success_list).to_csv(
        os.path.join(CSV_OUTPUT_FOLDER, "processed_frontpage_urls.csv"),
        index=False
    )
    logger.info(f"processed_frontpage_urls.csv saved to {CSV_OUTPUT_FOLDER}")
    pd.DataFrame().from_records(failure_list).to_csv(
        os.path.join(CSV_OUTPUT_FOLDER, "failed_to_screenshot_frontpage_urls.csv"),
        index=False
    )
    logger.info(f"failed_to_screenshot_frontpage_urls.csv saved to {CSV_OUTPUT_FOLDER}")

    logger.info("Program executed successfully. Exiting...")
    sys.exit(0)



if __name__ == "__main__":
    import os
    base_name = os.path.basename(__file__) 
    program_name = base_name if base_name != "main.py" else os.path.dirname(__file__)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"{program_name} program stopped.")


