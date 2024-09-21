# -*- coding: utf-8 -*-
"""ELM Web Scraping - Google search."""
import asyncio

from typing import Any

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

import pandas as pd

from config import *

from utils.search.google_search import PlaywrightGoogleLinkSearch

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
        self.search: PlaywrightGoogleLinkSearch = search
        self.results_list: list = []

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

    async def _insert_into_db(row):
        pass

    async def results(self, df: pd.DataFrame, db: MySqlDatabase, batch_size: int=INSERT_BATCH_SIZE)-> pd.DataFrame:
        """
        Perform an internet search for queries in the input DataFrame.

        ### Args
        - df: DataFrame containing 'gnis' and 'queries' columns.

        ### Returns
        - A DataFrame containing the URLS from the search results, as well as 'gnis', and their queries
        """
        import tqdm.asyncio as async_tqdm
        total_queries = df['queries'].str.len().sum()
        logger.info(f"Executing {total_queries} queries. This might take a while...")
        counter = 0

        async for row in async_tqdm(df.itertuples(), total=len(df), desc="Processing queries"): # Let's see what this does...
            async for query in row.queries:
                try:
                    results = await self.search.results(query)
                    logger.debug(f"Returned {len(results)} results for {query}: {results}")
                    results_dict = {
                        "gnis": row.gnis,
                        "queries": query,
                        "results": results
                    }
                    self.results_list.append(results_dict)
                except Exception as e:
                    logger.error(f"Error occurred while searching for query '{query}': {e}")

            return pd.DataFrame.from_dict(self.results_list)






