

import asyncio
import csv
import os
import subprocess
import sys
import time
from typing import Any

from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from tqdm.auto import tqdm as async_tqdm
import pandas as pd
import waybackpy

from logger.logger import Logger
logger = Logger(logger_name=__name__)

from config.config import INPUT_FILENAME, VERBOSITY, START, OUTPUT_FOLDER, DELAY, WAIT_TIME, DATABASE_NAME, ROUTE
from database.database import MySqlDatabase

from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.make_sha256_hash import make_sha256_hash
from utils.archive.reconstruct_domain_from_csv_filename import reconstruct_domain_from_csv_filename
from utils.archive.read_urls_from_csv import read_urls_from_csv
from utils.archive.read_domain_csv import read_domain_csv

# https://github.com/bitdruid/python-wayback-machine-downloader


# Path to the CSV file containing URLs
csv_file_path = f"./{INPUT_FILENAME}"


get_sources_sql_command = """
            SELECT gnis, 
                'municode' AS url, 
                source_municode AS value 
                FROM sources 
            WHERE source_municode IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'general_code' AS url, 
                source_general_code AS value 
                FROM sources 
            WHERE source_general_code IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'american_legal' AS url, 
                source_american_legal AS value 
                FROM sources 
            WHERE source_american_legal IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'code_publishing_co' AS url, 
                source_code_publishing_co AS value 
                FROM sources 
            WHERE source_code_publishing_co IS NOT NULL 
                UNION ALL 
            SELECT 
                gnis, 
                'place_domain' AS url, 
                source_place_domain AS value 
            FROM sources 
            WHERE source_place_domain IS NOT NULL;
            """



class SaveToInternetArchive:
    def __init__(self):
        self.db: MySqlDatabase = None
        self._get_ia_domains_sql: dict[str, str|dict[Any]] = {
            "sql": "SELECT DISTINCT domain FROM ia_url_metadata WHERE time_stamp < {one_year_ago}",
            "args": {"one_year_ago": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")}
        }
        self._get_ia_urls_sql: dict[str, str|dict[Any]] = { 
            "sql": "SELECT DISTINCT urls FROM ia_url_metadata WHERE time_stamp < {one_year_ago}",
            "args": {"one_year_ago": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")}
        }
        self._get_sources_sql: dict[str, str] = {
            "sql": get_sources_sql_command
        }
        self.waybackup_command = f"--current --list --verbosity {VERBOSITY} --start {START} --output {OUTPUT_FOLDER} --json --delay {DELAY}"


    async def _get_links_from_db(self, source: str=None) -> pd.DataFrame:
        if not source:
            logger.error("No source selected")
            raise ValueError("No source selected")
        match source:
            case "get_ia_domains":
                sql = self._get_ia_domains_sql['sql']
                args = self._get_ia_domains_sql['args']
            case "get_ia_urls":
                sql = self._get_ia_urls_sql['sql']
                args = self._get_ia_urls_sql['args']
            case "get_sources":
                sql = self._get_sources_sql['sql']
                args = None
            case "get_urls":
                sql = self._get_sources_sql['sql']
                args = None
            case _:
                logger.error("Unknown source selected")
                raise ValueError("Unknown source selected")
        if args:
            return await self.db.async_query_to_dataframe(sql)
        else:
            return await self.db.async_query_to_dataframe(sql,args=args)


    async def check(self, db: MySqlDatabase, wait_time: int=1) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.db = db
        urls_df = await self._get_links_from_db(source="get_sources")
        total_urls = len(urls_df)
        logger.info(f"{total_urls} URLs loaded. Starting waybackup.py...")

        counter = 0
        result_list = []
        no_result_list = []
        result_dict = {}
        for i, row in enumerate(urls_df.itertuples(), start=1):
            result_dict = {}
            result_dict['url'] = url = row.url
            result_dict['gnis'] = gnis = row.gnis

            # Build the waybackup command for the current URL
            command = f"waybackup --url {url} " + self.waybackup_command 
            command_list = command.split()

            # Run the waybackup command using subprocess
            try:
                logger.info(f"Running waybackup for URL {i} of {total_urls}: {url}")
                result_dict['result'] = result = subprocess.run(command_list, check=True)
                logger.info(f"URL {i} complete")
                logger.debug(f"result: {result}",t=10)
                counter += 1
            except subprocess.CalledProcessError as e:
                logger.error(f"Error occurred while processing {url}: {e}")
                logger.error(f"Error output: {e.stderr}")
            finally:
                logger.info(f"Waiting {wait_time} seconds...")
                time.sleep(wait_time)

            # If it returns results, save them to result_list. Else, save it to no_result_list
            if len(result) == 0:
                no_result_list.append(result_dict)
            else:
                result_list.append(result_dict)

        logger.info(f"Done! {counter} out of {total_urls} URLS were parsed successfully.")
        logger.info(f"{len(result_list)} URLS were on IA.\n{len(no_result_list)} URLs were not on IA. Returning dataframes...")
        on_ia_df = pd.DataFrame.from_dict()
        not_on_ia_df = pd.DataFrame.from_dict()
        return on_ia_df, not_on_ia_df


    async def save(self, db: MySqlDatabase, wait_time: int=1):
        self.db = db
        sources_df: pd.DataFrame = await self._get_links_from_db(source="get_sources")
        ia_url_metadata_df: pd.DataFrame = await self._get_links_from_db(source="get_ia_urls")

        total_urls = len(sources_df)
        logger.info(f"{total_urls} URLs loaded. Starting to save to Internet Archive...")
        user_agent = "Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Mobile Safari/537.36"
        counter = 0

        async_tqdm.pandas(desc="waybackpy URLs")
        pd.set_option("display.max_colwidth", 300)
        sources_df.progress_apply(lambda row: self._save_url(row, user_agent=user_agent, wait_time=wait_time, counter=counter))

    def _save_url(self, row, user_agent: dict=None, wait_time: int=1, counter: int=0):
        url = row.url
        gnis = row.gnis
        logger.info(f"Saving URL associated with GNIS {gnis}: {url}")
        try:
            wayback = waybackpy.Url(url, user_agent=user_agent)
            archive = wayback.save()
            logger.info(f"Successfully saved: {archive.archive_url}")
            counter += 1
        except Exception as e:
            logger.error(f"Error occurred while saving {url}: {e}")
        finally:
            logger.info(f"Waiting {wait_time} seconds...")
            time.sleep(wait_time)


        # for i, row in enumerate(sources_df.itertuples(), start=1):
        #     url = row.url
        #     gnis = row.gnis

        #     # Construct the wayback machine save URL
        #     save_url = f"https://web.archive.org/save/{url}"
        #     try:
        #         logger.info(f"Saving URL {i} of {total_urls}: {url}")
        #         headers = {
        #             "api": "NA"
        #         }
        #         # Use aiohttp to send an asynchronous GET request to save the URL.
        #         async with aiohttp.ClientSession() as session:
        #             async with session.get(save_url, headers=headers) as response:
        #                 if response.status == 200:
        #                     logger.info(f"Successfully saved: {url}")
        #                     counter += 1
        #                 else:
        #                     logger.warning(f"Failed to save: {url}. Status code: {response.status}")

        #     except Exception as e:
        #         logger.error(f"Error occurred while saving {url}: {str(e)}")

        #     finally:
        #         await asyncio.sleep(DELAY) # Wait between requests to avoid overwhelming the server




















async def insert_into_mysql(input_method: str="mysql"):
    logger.info("ROUTE: insert_into_mysql selected.")
    folder = Path(OUTPUT_FOLDER)
    counter = 0

    async with MySqlDatabase(database=DATABASE_NAME) as db:

        if input_method == "mysql":
            args = {
                "one_year_ago": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
            }
            # Get domains with time_stamps that are older than a year ago today.
            db_domains = await db.async_execute_sql_command(
                "SELECT domain FROM ia_url_metadata WHERE time_stamp < {one_year_ago}",
                args=args
            )
            
        else:
            # Get a list of domains that we've already processed.
            db_domains = await db.async_execute_sql_command(
                "SELECT DISTINCT domain FROM ia_url_metadata"
            )
            if len(db_domains) > 0:
                db_domains_changes = [
                    str(domain[0]) for domain in db_domains # Change the list of tuples into a list of strings.
                ]
                db_domains = db_domains_changes
            else:
                # If there's nothing in the list, just append a string to an empty list.
                db_domains = []
                db_domains.append("")

            for file in folder.iterdir():
                file_name = reconstruct_domain_from_csv_filename(file.name)

                if file.suffix.lower() == '.csv' and file_name not in db_domains:

                    logger.info(f"Reading {file} into Python..")
                    mysql_input = read_domain_csv(file) # -> list[tuple[ia_id, time_stamp, digest, mimetype, http_status, url]]
                    logger.info(f"Read successful. Inserting into MySQL database '{DATABASE_NAME}'...")

                    args = {
                        "sql_column_names": "ia_id, time_stamp, digest, mimetype, http_status, url, domain"
                    }

                    try:
                        await db.async_execute_sql_command(
                            "INSERT INTO ia_url_metadata ({sql_column_names}) VALUES (%s, %s, %s, %s, %s, %s, %s);",
                            params=mysql_input, 
                            args=args
                        )
                        logger.info(f"Contents of {file} inserted into MySQL database '{DATABASE_NAME}' successfully.")
                        counter += 1
                    except Exception as e:
                        logger.exception(f"An error occurred: {e}")
                        raise e
                else:
                    if file.suffix.lower() == '.csv':
                        logger.debug(f"file '{file.name}' already processed. Skipping...")
                    else:
                        logger.debug(f"file '{file.name}' is not a csv. Skipping...")

    logger.info(f"Done! {counter} csvs were successfully inserted into MySQL.")



async def check_internet_archive(input_method="mysql",output_method="mysql"):
    pass


async def main():

    if ROUTE == "insert_into_mysql":
        await insert_into_mysql(input_method="mysql")

    elif ROUTE == "check_internet_archive": # Check internet archive route.
        results = None
        logger.info("ROUTE: check_internet_archive selected.")

        urls = read_urls_from_csv(csv_file_path)
        counter = 0

        # Filter out URLs that we already have CSV's for.
        folder = Path(OUTPUT_FOLDER)
        existing_files = {file.stem for file in folder.glob('*.csv')}
        urls_to_process = []

        for url in urls:
            sanitized_url = "waybackup_" + sanitize_filename(url)
            # logger.debug(f"sanitized_url: {sanitized_url}")
            if sanitized_url in existing_files:
                pass
                # logger.info(f"csv file for URL {url} already present. Skipping...")
            else:
                urls_to_process.append(url)

        urls = urls_to_process
        total_urls = len(urls)
        logger.info(f"{total_urls} URLs loaded. Starting waybackup.py...")
        time.sleep(5)

        for i, url in enumerate(urls):

            # Build the command for the current URL
            command = f"waybackup --url {url} " + DEFAULT
            command_list = command.split()

            # Run the waybackup command using subprocess
            try:
                logger.info(f"Running waybackup for URL {i} of {total_urls}: {url}")
                result = subprocess.run(command_list, check=True)
                logger.info(f"Completed: {url}")
                counter += 1
                logger.info(f"Waiting {WAIT_TIME} seconds...")
                time.sleep(WAIT_TIME)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error occurred while processing {url}: {e}")
                logger.error(f"Error output: {e.stderr}")
                logger.info(f"Waiting {WAIT_TIME} seconds...")
                time.sleep(WAIT_TIME)

        logger.info(f"Done! {counter} out of {total_urls} URLS were parsed successfully.")

    else:
        logger.info("No route was selected. Exiting...")

    sys.exit()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("check_ia subprocess stopped.")


# waybackup_command_base = [
#     "--current", CURRENT,
#     "--full", FULL,
#     "--save", SAVE,
#     "--list", LIST,
#     "--explicit", EXPLICIT,
#     "--output", OUTPUT,
#     "--range", RANGE,
#     "--start", START,
#     "--end", END,
#     "--filetype", FILETYPE,
#     "--csv", CSV,
#     "--skip", SKIP,
#     "--no-redirect", NO_REDIRECT,
#     "--verbosity", VERBOSITY,
#     "--log", LOG,
#     "--retry", RETRY,
#     "--workers", WORKERS,
#     "--delay", DELAY,
#     "--limit", LIMIT,
#     "--cdxbackup", CDX_BACKUP,
#     "--cdxinject", CDX_INJECT,
#     "--auto", AUTO
# ]

