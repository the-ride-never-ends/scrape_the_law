

import asyncio
import csv
import os
import subprocess
import sys
import time

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from logger import Logger
logger = Logger(logger_name=__name__)

from config import *
from database import MySqlDatabase

from utils.archive.sanitize_filename import sanitize_filename
from utils.archive.reconstruct_domain_from_csv_filename import reconstruct_domain_from_csv_filename
from utils.archive.read_urls_from_csv import read_urls_from_csv


# https://github.com/bitdruid/python-wayback-machine-downloader




# Path to the CSV file containing URLs
csv_file_path = f"./{INPUT_FILENAME}"

DEFAULT = f"--current --list --verbosity {VERBOSITY} --start {START} --output {OUTPUT_FOLDER} --csv --delay {DELAY}"






class SaveToInternetArchive:
    def __init__(self,
                 db: MySqlDatabase
                ):
        self.db = db


    async def _get_domains(self) -> pd.DataFrame:
        safe_format_vars = {
            "one_year_ago": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
        }

        # Get domains with time_stamps that are older than a year ago today.
        df = await self.db.async_query_to_dataframe(
            "SELECT domain FROM ia_url_metadata WHERE time_stamp < {one_year_ago}",
            safe_format_vars=safe_format_vars
        )
        return df

    async def check(self):
        urls_df = await self._get_domains()
        total_urls = len(urls_df)
        logger.info(f"{total_urls} URLs loaded. Starting waybackup.py...")


        for i, row in enumerate(urls_df.itertuples(), start=1):
            url = row.url
            # Build the command for the current URL
            command = f"waybackup --url {url} " + DEFAULT
            command_list = command.split()

            # Run the waybackup command using subprocess
            try:
                logger.info(f"Running waybackup for URL {i} of {total_urls}: {url}")
                result = subprocess.run(command_list, check=True)
                logger.info(f"URL {i} complete")
                counter += 1
                logger.info(f"Waiting {WAIT_TIME} seconds...")
                time.sleep(WAIT_TIME)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error occurred while processing {url}: {e}")
                logger.error(f"Error output: {e.stderr}")
                logger.info(f"Waiting {WAIT_TIME} seconds...")
                time.sleep(WAIT_TIME)

        logger.info(f"Done! {counter} out of {total_urls} URLS were parsed successfully.")


    async def save():
        pass









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
                            safe_format_vars=args
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

