# -*- coding: utf-8 -*-
"""ELM Web Scraping - Google search."""
import asyncio

from typing import Any

import pandas as pd
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)
import tqdm.asyncio as async_tqdm

from config import INSERT_BATCH_SIZE, SEARCH_ENGINE

from utils.search.google_search import PlaywrightGoogleLinkSearch
from utils.shared.get_formatted_datetime import get_formatted_datetime
from utils.shared.make_sha256_hash import make_sha256_hash
from utils.shared.convert_integer_to_datetime_str import convert_integer_to_datetime_str
from utils.database.get_insert_into_values import get_insert_into_values

from database import MySqlDatabase
from logger import Logger
log_level=10
logger = Logger(logger_name=__name__,log_level=log_level)



# Set up rate-limit-conscious functions
CONCURRENCY_LIMIT = 2
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
stop_signal = False



async def run_task_with_limit(task):
    async with semaphore:
        result = await task
        if result == "stop_condition":  # Replace with your specific stop condition
            global stop_signal
            stop_signal = True
        return result


    # queries = [
    #     'site:https://library.municode.com/ca/ Camarillo tax', "City of Honeyville UT"
    # ]


class SearchEngine:
    """
    A class representing a search engine interface for various search providers.

    This class provides a unified interface for performing web searches using different
    search engines, with Google as the default option.

    - start_engine(cls, search_engine, launch_kwargs): Create and return a SearchEngine instance for the specified search engine.
    - results(self, df: pd.DataFrame) -> pd.DataFrame: Perform a search for queries in the provided DataFrame and return the results.

    """

    def __init__(self, search: PlaywrightGoogleLinkSearch):
        """
        Initialize the SearchEngine instance.

        Args:
            search: An instance of a specific search engine class (e.g., PlaywrightGoogleLinkSearch).
        """
        self.search = search
        self.results_list = []
        self.results_set = set()
        self.urls_list = []
        self.urls_set = []

    @classmethod
    def start_engine(cls, search_engine:str, launch_kwargs: dict[str,Any]) -> 'SearchEngine':
        """
        Create and return a SearchEngine instance for the specified search engine.

        ### Args
        - search_engine (str): The name of the search engine to use (e.g., "google", "bing").
        - launch_kwargs (dict): Keyword arguments to pass to the search engine class constructor.

        ### Returns
        - SearchEngine: An instance of the SearchEngine class configured for the specified search engine.

        ### Raises
        - NotImplementedError: If the specified search engine is not implemented yet.
        """
        match search_engine:
            case "google":
                search = PlaywrightGoogleLinkSearch(**launch_kwargs)
            case "bing":
                raise NotImplementedError("PlaywrightBingLinkSearch class not implemented yet.")
            case "duckduckgo":
                raise NotImplementedError("PlaywrightDuckDuckGoLinkSearch class not implemented yet.")
            case "brave":
                raise NotImplementedError("PlaywrightBraveLinkSearch class not implemented yet.")
            case _:
                # Default to Google search
                logger.warning(f"Unknown search engine type: {search_engine}. Defaulting to Google.")
                search = PlaywrightGoogleLinkSearch(**launch_kwargs)
        return cls(search)


    async def _batched_search_results(self, gnis: str, group: pd.DataFrame):
        queries = group['query'].tolist()
        type_query = query[0]
        type_individual = query[0][0]
        assert isinstance(type_query, list), f"query not a list, but {type(type_query)}"
        assert isinstance(type_individual, str), f"individual query not a string, but {type(type_individual)}"
        source = group['source'].iloc[0]

        try:
            results = await self.search.results(queries)
            formatted_datetime = get_formatted_datetime()
            for query, result in zip(queries, results):
                query_hash = make_sha256_hash(gnis, query, formatted_datetime)
                results_dict = {
                    "query_hash": query_hash,
                    "gnis": gnis,
                    "query_text": query,
                    "num_results": len(result),
                    "source_site": source,
                    "search_engine": SEARCH_ENGINE,
                    "time_stamp": formatted_datetime
                }
                for url in result: 
                    if not url in self.results_set:
                        self.results_set.add(url)
                        urls_dict = {
                            "url_hash": make_sha256_hash(gnis, url),
                            "query_hash": query_hash,
                            "gnis": gnis,
                            "url": url
                        }
                        self.urls_list.append(urls_dict)
                        logger.debug(f"url '{url}' added to urls_list")
                self.results_list.append(results_dict)
                logger.debug(f"query '{query}' added to results_list")
        except Exception as e:
            logger.error(f"Error occurred while searching for GNIS {gnis}: {e}")


    async def results(self, df: pd.DataFrame, db: MySqlDatabase, batch_size: int=INSERT_BATCH_SIZE)-> pd.DataFrame:
        """
        Perform an internet search for queries in the input DataFrame.

        ### Args
        - df: DataFrame containing 'gnis' and 'query' columns.

        ### Returns
        - A DataFrame containing the URLS from the search results, as well as 'gnis', and their queries

        # 2024-09-21 22:43:34,897 - __main___logger - DEBUG - main.py: 56 - main queries_df:
        #       gnis                                            query            source
        # 0  2409449  site:https://library.municode.com/tx/childress...        municode
        # 1  2390602  site:https://ecode360.com/ City of Hurricane W...    general_code
        # 2  2412174  site:https://codelibrary.amlegal.com/codes/wal...  american_legal
        # 3   885195           site:https://demarestnj.org/ "sales tax"    place_domain
        # 4  2411145  site:https://library.municode.com/ca/monterey/...        municode
        """
        # Initialize for-loop variables.
        total_queries = len(df)
        logger.info(f"Executing {total_queries} queries. This might take a while...")

        # Group the DataFrame by 'gnis'
        grouped = df.groupby('gnis')

        for gnis, groups in grouped:
            await self._batched_search_results(gnis, groups)
            await db.async_insert_by_batch(self.results_list)
            self.results_list = None # Clear the results list at the end of the loop

        query_batch = []
        reset = True
        row_count = 0
        idx = 1
        df.groupby()


        async for row in async_tqdm(df.itertuples(), total=len(df), desc="Processing queries"): # Let's see what this does...
            if first_run: # Define benchmark on first run.
                reference_gnis = row.gnis
                first_run = False
                logger.debug(f"Start of batch {idx} reference_gnis: {reference_gnis}, first_run: {first_run}")
                continue
            else:
                if row.gnis in query_batch[0][0]:
                    query_batch.append(row.query)
                else:
                    reference_gnis = row.gnis # Set the reference to the next gnis group.

                if len(query_batch) >= 10:
                    try:
                        query_batch.append(query_batch)
                        results: list[list[str]] = await self.search.results(query_batch)
                        async for result in results:
                            results_dict = {
                                "query_hash": make_sha256_hash(row.gnis),
                                "gnis": row.gnis,
                                "queries": query,
                                "results": results,
                                "num_results": len_results,
                                "source_site": row.source,
                                "search_engine": SEARCH_ENGINE,
                                "time_stamp": get_formatted_datetime()
                            }
                            len_results = len(results)
                            logger.debug(f"Returned {len_results} results for row {idx} '{query}': {results}")
                            results_dict = {
                                "query_hash": make_sha256_hash(row.gnis),
                                "gnis": row.gnis,
                                "queries": query,
                                "results": results,
                                "num_results": len_results,
                                "source_site": row.source,
                                "search_engine": SEARCH_ENGINE,
                                "time_stamp": get_formatted_datetime()
                            }
                        self.results_list.append(results_dict)
                        row_count += len_results
                        idx += 1
                    except Exception as e:
                        logger.error(f"Error occurred while searching for query '{query}': {e}")
                    finally:
                        query_batch = []
                else:
                    continue

                if row_count >= 10 or INSERT_BATCH_SIZE:

        return pd.DataFrame.from_dict(self.results_list)






