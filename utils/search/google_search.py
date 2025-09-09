# -*- coding: utf-8 -*-
"""ELM Web Scraping - Google search."""
import asyncio
import os
import re
import time
import traceback

from playwright.async_api import (
    async_playwright,
    Playwright as AsyncPlaywright,
    Page as PlaywrightPage,
    TimeoutError as PlaywrightTimeoutError,
)

from config.config import GOOGLE_CONCURRENCY_LIMIT, GOOGLE_SEARCH_RESULT_TAG, DEBUG_FILEPATH
from utils.query.clean_search_query import clean_search_query
from utils.shared.make_id import make_id
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.safe_format import safe_format

from utils.shared.limiter_utils.Limiter import Limiter
limiter  = Limiter(GOOGLE_CONCURRENCY_LIMIT)

from .google_search_utils.navigate_to_google import navigate_to_google
from .google_search_utils.perform_google_search import perform_google_search
from .google_search_utils.extract_links import extract_links

from logger.logger import Logger
log_level=10
logger = Logger(logger_name=__name__,log_level=log_level)

_pw_debug_path = "/mnt/e/AI_TEMP/scrape_the_law_debug/"
pw_debug_path = os.path.join(_pw_debug_path, "playwright")

if not os.path.exists(pw_debug_path):
    os.mkdir(pw_debug_path)


class PlaywrightGoogleLinkSearch:
    """
    Search for top results on google and return their links.\n
    NOTE This has been heavily modified from ELM's original code. We'll see if it's more effective in the long run.

    Parameters
    ----------
    **launch_kwargs
        Keyword arguments to be passed to
        `playwright.chromium.launch`. For example, you can pass
        ``headless=False, slow_mo=50`` for a visualization of the
        search.
    
    """

    EXPECTED_RESULTS_PER_PAGE = 10
    """Number of results displayed per Google page. """

    def __init__(self, **launch_kwargs):
        """
        Initialize the PlaywrightGoogleLinkSearch instance.
        
        Sets up a Google search instance using Playwright for web automation.
        The browser is not launched immediately but will be created when search
        operations are performed.
        
        Args:
            **launch_kwargs: Keyword arguments to be passed to
                playwright.chromium.launch(). For example, you can pass
                headless=False, slow_mo=50 for a visualization of the search.
        
        Returns:
            None
        
        Raises:
            TypeError: If invalid launch_kwargs are provided.
        
        Example:
            >>> searcher = PlaywrightGoogleLinkSearch(headless=False, slow_mo=50)
            >>> searcher = PlaywrightGoogleLinkSearch()  # Default settings
        """
        """
        Parameters
        ----------
        **launch_kwargs
            Keyword arguments to be passed to
            `playwright.chromium.launch`. For example, you can pass
            ``headless=False, slow_mo=50`` for a visualization of the
            search.
        """
        self.launch_kwargs = launch_kwargs
        self._browser = None


    async def _load_browser(self, pw_instance: AsyncPlaywright):
        """
        Load and initialize a Chromium browser instance.
        
        Creates a new browser instance using the provided Playwright instance
        and the launch arguments specified during initialization.
        
        Args:
            pw_instance (AsyncPlaywright): The Playwright instance to use for
                launching the browser.
        
        Returns:
            None
        
        Raises:
            playwright.async_api.Error: If browser launch fails.
            ConnectionError: If unable to connect to browser.
        
        Example:
            >>> async with async_playwright() as pw:
            ...     await self._load_browser(pw)
        """
        """Launch a chromium instance and load a page"""
        self._browser = await pw_instance.chromium.launch(**self.launch_kwargs)


    async def _close_browser(self):
        """
        Close the browser instance and reset internal state.
        
        Closes the currently active browser instance and resets the internal
        browser reference to None for cleanup.
        
        Args:
            None
        
        Returns:
            None
        
        Raises:
            AttributeError: If browser is None or already closed.
            playwright.async_api.Error: If browser close operation fails.
        
        Example:
            >>> await self._close_browser()
        """
        """Close browser instance and reset internal attributes"""
        await self._browser.close()
        self._browser = None


    async def _search(self, query, num_results=10):
        """
        Perform a single Google search query and extract links.
        
        Executes a Google search for the given query and extracts the top search
        result URLs. The number of results is limited to the maximum results per
        page (typically 10). Handles debug mode with additional logging and screenshots.
        
        Args:
            query (str): The search query string to execute on Google.
            num_results (int, optional): Maximum number of results to retrieve.
                Defaults to 10. Cannot exceed EXPECTED_RESULTS_PER_PAGE.
        
        Returns:
            list[str]: List of URLs from the search results, or empty list if
                no results found.
        
        Raises:
            PlaywrightTimeoutError: If the search operation times out.
            AttributeError: If browser is not loaded.
        
        Example:
            >>> results = await self._search("python programming", num_results=5)
            >>> print(f"Found {len(results)} results")
        """
        """Search google for links related to a query."""
        logger.debug(f"Searching Google: {query}")
        num_results = min(num_results, self.EXPECTED_RESULTS_PER_PAGE)

        if log_level == 10: # Trace debugging chunk
            context = await self._browser.new_context()
            await navigate_to_google(page, context=context)
            await perform_google_search(page, query, context=context)
            return await extract_links(page, query)

        else:
            page = await self._browser.new_page()
            await navigate_to_google(page)
            await perform_google_search(page, query)
            return await extract_links(page, query)


    async def _skip_exc_search(self, query, num_results=10):
        """
        Perform a Google search with timeout exception handling.
        
        Executes a Google search while gracefully handling PlaywrightTimeoutError
        exceptions. If a timeout occurs, logs the error and returns an empty list
        instead of raising the exception. Measures and logs execution time.
        
        Args:
            query (str): The search query string to execute on Google.
            num_results (int, optional): Maximum number of results to retrieve.
                Defaults to 10.
        
        Returns:
            list[str]: List of URLs from the search results, or empty list if
                timeout occurs or no results found.
        
        Raises:
            AttributeError: If browser is not loaded.
            Exception: Any non-timeout related exceptions are re-raised.
        
        Example:
            >>> results = await self._skip_exc_search("legal documents")
            >>> if not results:
            ...     print("Search timed out or no results found")
        """
        """Perform search while ignoring timeout errors"""
        try:
            start = time.time()
            results = await self._search(query, num_results=num_results)
            execution_time = time.time() - start
            logger.debug(f"Query '{query}' took {execution_time} seconds to complete.")
            return results
        except PlaywrightTimeoutError as e:
            logger.info(f"Google timed-out for query '{query}'. Returning empty list...")
            #logger.exception(e)
            #traceback.print_exc()
            return []


    async def _get_links(self, queries, num_results):
        """
        Execute multiple Google search queries concurrently without rate limiting.
        
        Performs Google searches for multiple queries simultaneously using asyncio
        tasks. Manages browser lifecycle (load/close) and executes all searches
        concurrently without concurrency limits.
        
        Args:
            queries (iterable): Collection of search query strings to execute.
            num_results (int): Maximum number of results to retrieve per query.
        
        Returns:
            list[list[str]]: List where each element is a list of URLs corresponding
                to the search results for each query in the same order.
        
        Raises:
            playwright.async_api.Error: If browser operations fail.
            asyncio.TimeoutError: If asyncio.gather times out.
        
        Example:
            >>> queries = ["python programming", "web scraping"]
            >>> results = await self._get_links(queries, 5)
            >>> for i, query_results in enumerate(results):
            ...     print(f"Query {i}: {len(query_results)} results")
        """
        """Get links for multiple queries"""
        outer_task_name = asyncio.current_task().get_name()
        async with async_playwright() as pw_instance:
            await self._load_browser(pw_instance)
            searches = [
                asyncio.create_task(
                    self._skip_exc_search(query, num_results=num_results),
                    name=outer_task_name,
                )
                for query in queries
            ]
            results = await asyncio.gather(*searches)
            await self._close_browser()
        return results


    async def _get_links_with_limit(self, queries, num_results):
        """
        Execute multiple Google search queries with concurrency rate limiting.
        
        Performs Google searches for multiple queries using a concurrency limiter
        to respect rate limits and avoid overwhelming Google's servers. Manages
        browser lifecycle and applies the configured concurrency limit.
        
        Args:
            queries (iterable): Collection of search query strings to execute.
            num_results (int): Maximum number of results to retrieve per query.
        
        Returns:
            list[list[str]]: List where each element is a list of URLs corresponding
                to the search results for each query in the same order.
        
        Raises:
            playwright.async_api.Error: If browser operations fail.
            asyncio.TimeoutError: If asyncio.gather times out.
        
        Example:
            >>> queries = ["legal documents", "court cases", "legislation"]
            >>> results = await self._get_links_with_limit(queries, 10)
            >>> total_links = sum(len(query_results) for query_results in results)
            >>> print(f"Retrieved {total_links} total links")
        """
        """Get links for multiple queries with a concurrency limiter"""
        outer_task_name = asyncio.current_task().get_name()
        async with async_playwright() as pw_instance:
            await self._load_browser(pw_instance)
            searches = [
                asyncio.create_task(
                    self._skip_exc_search(query, num_results=num_results),
                    name=outer_task_name,
                )
                for query in queries
            ]
            searches_with_limit = [
                limiter.run_task_with_limit(search) for search in searches
            ]
            results = await asyncio.gather(*searches_with_limit)
            await self._close_browser()
        return results


    async def results(self, *queries, num_results=10, limit=True):
        """
        Retrieve Google search results for multiple queries.
        
        This is the main public method that executes Google searches for the provided
        queries and returns lists of URLs for each query. Supports both rate-limited
        and unlimited concurrency modes. Automatically cleans search queries before
        execution.
        
        Args:
            *queries: Variable number of search query strings to execute.
            num_results (int, optional): Number of top results to retrieve for each
                query. Cannot exceed EXPECTED_RESULTS_PER_PAGE (typically 10).
                Defaults to 10.
            limit (bool, optional): Whether to apply concurrency rate limiting.
                Defaults to True for respectful API usage.
        
        Returns:
            list[list[str]]: List equal to the length of input queries, where each
                entry is another list containing the top num_results links for
                that query.
        
        Raises:
            ValueError: If no queries are provided.
            playwright.async_api.Error: If browser operations fail.
        
        Example:
            >>> searcher = PlaywrightGoogleLinkSearch()
            >>> results = await searcher.results("python", "javascript", num_results=5)
            >>> for i, links in enumerate(results):
            ...     print(f"Query {i+1}: {len(links)} links found")
        """
    """
        """Retrieve links for the first `num_results` of each query.

        This function executes a google search for each input query and
        returns a list of links corresponding to the top `num_results`.

        Parameters
        ----------
        num_results : int, optional
            Number of top results to retrieve for each query. Note that
            this value can never exceed the number of results per page
            (typically 10). If you pass in a larger value, it will be
            reduced to the number of results per page.
            By default, ``10``.

        Returns
        -------
        list
            List equal to the length of the input queries, where each
            entry is another list containing the top `num_results`
            links.
        """
        logger.debug(f"queries_type: {type(queries)}")
        queries = map(clean_search_query, *queries)
        if limit:
            return await self._get_links_with_limit(queries, num_results)
        else:
            return await self._get_links(queries, num_results)


