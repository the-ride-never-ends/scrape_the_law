# -*- coding: utf-8 -*-
""" Scrape a Search Engine """
import asyncio
import time
from typing import Any, Coroutine


import pandas as pd
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)
#from tqdm import asyncio as tqdmasyncio


from config.config import INSERT_BATCH_SIZE, SEARCH_ENGINE


from utils.search.google_search import PlaywrightGoogleLinkSearch
from utils.shared.get_formatted_datetime import get_formatted_datetime
from utils.shared.make_sha256_hash import make_sha256_hash
#from utils.shared.convert_integer_to_datetime_str import convert_integer_to_datetime_str
#from utils.database.get_insert_into_values import get_insert_into_values
#from utils.archive.sanitize_filename import sanitize_filename


from database.database import MySqlDatabase
from logger.logger import Logger
log_level = 10
logger = Logger(logger_name=__name__,log_level=log_level)


from utils.shared.limiter_utils.Limiter import Limiter
CONCURRENCY_LIMIT = 2
limiter = Limiter(CONCURRENCY_LIMIT)


class SearchEngine:
    """
    A class representing a search engine interface for various search providers.

    This class provides a unified interface for performing web searches using different
    search engines. Defaults to Google.

    - start_engine(cls, search_engine, launch_kwargs): Create and return a SearchEngine instance for the specified search engine.
    - results(self, df: pd.DataFrame) -> pd.DataFrame: Perform a search for queries in the provided DataFrame and return the results.

    """

    def __init__(self, search=None):
        """
        Initialize the SearchEngine instance.

        Args:
            search: An instance of a specific search engine class (e.g., PlaywrightGoogleLinkSearch).
        """
        self.search = search
        self.queries_list = []
        self.urls_list = []
        self.url_hash_set = set()
        self.query_hash_set = set()
        self.db = None
        self.sql_queries: dict = {
            "url_hash": "SELECT DISTINCT url_hash FROM urls WHERE gnis = {gnis};",
            "query_hash": "SELECT DISTINCT query_hash FROM searches WHERE gnis = {gnis};",
            "url": "SELECT * FROM urls WHERE ia_url IS NULL;"
        }

    @classmethod
    def start_engine(cls, search_engine:str, **launch_kwargs) -> 'SearchEngine':
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


    async def _process_search_result(self, gnis: int, query_text: str, result: list[str], source_site: str) -> None:
        """
        Build dictionaries of query metadata and returned URLs and add them to their respective lists.
        """
        # Make the table 'searches' insert dictionary.
        formatted_datetime = get_formatted_datetime()
        query_hash = make_sha256_hash(gnis, query_text, SEARCH_ENGINE)
        result_dict = {
            "query_hash": query_hash,
            "gnis": gnis,
            "query_text": query_text,
            "num_results": len(result),
            "source_site": source_site,
            "search_engine": SEARCH_ENGINE,
            "time_stamp": formatted_datetime
        }
        if len(result) != 0:
            for url in result:
                url_hash = make_sha256_hash(gnis, url)

                # Check the results_set to see if the url is in it.
                # If it isn't, make the insert dictionary.
                if url_hash not in self.url_hash_set:
                    self.url_hash_set.add(url)
                    urls_dict = {
                        "url_hash": url_hash,
                        "query_hash": query_hash,
                        "gnis": gnis,
                        "url": url
                    }
                    self.urls_list.append(urls_dict)
                    logger.debug(f"URL '{url}' added to urls_list")
        else:
            logger.info(f"Query '{query_text}' returned no results")

        self.queries_list.append(result_dict)
        logger.debug(f"query '{query_text}' added to queries_list")


    async def _batched_search_results(self, gnis: int, group_df: pd.DataFrame) -> None:
        """
        Run search queries through a pre-specified search engine class, get the results, then process them and add them to a list.
        """
        queries = group_df['query'].tolist()
        source_sites = group_df['source'].tolist()

        try:
            search_results: list[list[str]] = await self.search.results(queries)
            logger.debug(f"search_results:\n{search_results}")
            logger.info(f"Got search results. Processing...")
            await asyncio.gather(*[
                self._process_search_result(
                    gnis, 
                    query_text, 
                    result, 
                    source_site
                    ) for query_text, result, source_site in zip(queries, search_results, source_sites)
            ])
        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout occurred while searching for GNIS {gnis}: {e}")
        except asyncio.InvalidStateError as e:
            logger.error(f"Invalid State error occurred while searching for GNIS {gnis}: {e}")
        except Exception as e:
            logger.error(f"Error occurred while searching for GNIS {gnis}: {e}")


    async def _get_urls_without_ia_url(self) -> pd.DataFrame:
        """
        Get all URLs where we don't have an internet archive URL from the database
        """
        urls = await self.db.async_execute_sql_command(self.sql_queries["url"], return_dict=True)
        return pd.DataFrame.from_dict(urls)


    async def _get_hash_by_gnis(self, query: str, gnis: int) -> set:
        """
        Get hashes based on gnis and return them as a set.
        """
        hashes = await self.db.async_execute_sql_command(query, args={"gnis": gnis})
        return  {row[0] for row in hashes}


    async def _insert_batched_data(self, batch_size: int=20):
        """
        Insert URLs and queries if they go over the batch size, then clear them.
        """
        if len(self.urls_list) >= batch_size:
            await self.db.async_insert_by_batch(self.urls_list, table="urls", batch_size=batch_size)
            self.urls_list = []

        if len(self.queries_list) >= batch_size:
            await self.db.async_insert_by_batch(self.queries_list, table="searches", batch_size=batch_size)
            self.queries_list = []


    async def results(self, df: pd.DataFrame, batch_size: int=INSERT_BATCH_SIZE, skip_seach=False)-> pd.DataFrame:
        """
        Perform an internet search for queries in the input DataFrame.

        ### Args
        - df: DataFrame containing 'gnis', 'query', and 'source' columns.
        - batch_size:

        ### Returns
        - A DataFrame containing the URLS from the search results, as well as 'gnis', and their queries

        ### Example Input
        >>> 2024-09-21 22:43:34,897 - __main___logger - DEBUG - main.py: 56 - main queries_df:
        >>>       gnis                                              query          source                                         query_hash
        >>> 0  2409449  site:https://library.municode.com/tx/childress...        municode  511e95fbc8e3bd151ee2f0e7154127b9254d3c3baa7038...
        >>> 1  2390602  site:https://ecode360.com/ City of Hurricane W...    general_code  d3d5ded19690fe0a4e2f60db0764698517f6c53f08df3b...
        >>> 2  2412174  site:https://codelibrary.amlegal.com/codes/wal...  american_legal  4512ed10a5f5090fac09cda3fc83d82696dce9631cddb0...
        >>> 3   885195           site:https://demarestnj.org/ "sales tax"    place_domain  d2f5127d50661f6fee35f61e8c645e02516f6713a13bc8...
        >>> 4  2411145  site:https://library.municode.com/ca/monterey/...        municode  1ea0e4a28ae808645537068ce4ff009973e2d48cd95db5...
        """
        # Type check the dataframe
        required_columns = {'gnis', 'query', 'source', 'query_hash'}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")

        # Initialize for-loop variables.
        total_queries = len(df)

        logger.info(f"Executing {total_queries} queries. This might take a while...")
        # Group the DataFrame by geographic ID 'gnis'
        async with MySqlDatabase(database="socialtoolkit") as db:
            self.db = db
            try:
                for gnis, groups_df in df.groupby('gnis'): 
                    # Get the list of queries and URL's we've already performed/got for this gnis.
                    logger.info(f"Gettings url hashes for gnis '{gnis}'...")
                    self.url_hash_set = await self._get_hash_by_gnis(self.sql_queries["url_hash"], gnis)
                    self.query_hash_set = await self._get_hash_by_gnis(self.sql_queries["query_hash"], gnis)

                    # Filter out the queries we've already performed
                    if len(self.query_hash_set) > 0:
                        groups_df[~groups_df['query_hash'].isin(list(self.query_hash_set))]
                        logger.info(f"Filtered out {len(groups_df)} redundant queries")

                    #logger.debug(f"search groups_df with gnis '{gnis}':\n{groups_df.head()}")

                    # Stop the process if in debug to see the results.
                    logger.debug(f"self.url_hash_set:\n{self.url_hash_set}")
                    if log_level == 10:
                        logger.debug("LET'S FUCKING GOOOOOOO!!!!")
                        time.sleep(1)

                    # Perform the search
                    # NOTE We don't paginate results as the search classes will never go beyond the first page of results.
                    start = time.time()
                    await self._batched_search_results(gnis, groups_df)
                    execution_time = time.time() - start
                    logger.debug(f"*** {len(groups_df)} Queries took {execution_time} seconds to complete. ***")

                    # Insert data in smaller batches for more steady input
                    await self._insert_batched_data(batch_size=20)

            except asyncio.CancelledError: 
                logger.info("Keyboard Interupt was called. Saving remaining data...")
            finally:
                # Ensure any remaining data is saved
                await self._insert_batched_data(batch_size=1)  # Insert all remaining items

            # Insert any remaining items
            if self.urls_list:
                await self.db.async_insert_by_batch(self.urls_list, batch_size=1)
            if self.queries_list:
                await self.db.async_insert_by_batch(self.queries_list, batch_size=1)

        logger.info(f"'{SEARCH_ENGINE}' search complete")
        # Since searches will likely occur over time, we get URLs for the next step from the database instead.
        return await self._get_urls_without_ia_url()

