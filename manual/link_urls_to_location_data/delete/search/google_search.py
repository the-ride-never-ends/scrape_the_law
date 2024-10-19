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

from config import GOOGLE_CONCURRENCY_LIMIT, GOOGLE_SEARCH_RESULT_TAG, DEBUG_FILEPATH
from utils.query.clean_search_query import clean_search_query
from utils.shared.make_id import make_id
from utils.shared.sanitize_filename import sanitize_filename
from utils.shared.safe_format import safe_format

from utils.shared.limiter_utils.Limiter import Limiter
limiter  = Limiter(GOOGLE_CONCURRENCY_LIMIT)

from .google_search_utils.navigate_to_google import navigate_to_google
from .google_search_utils.perform_google_search import perform_google_search
from .google_search_utils.extract_links import extract_links

from logger import Logger
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
        """Launch a chromium instance and load a page"""
        self._browser = await pw_instance.chromium.launch(**self.launch_kwargs)


    async def _close_browser(self):
        """Close browser instance and reset internal attributes"""
        await self._browser.close()
        self._browser = None


    async def _search(self, query, num_results=10):
        """Search google for links related to a query."""
        logger.debug(f"Searching Google: {query}")
        num_results = min(num_results, self.EXPECTED_RESULTS_PER_PAGE)

        if log_level == 10: # Trace debugging chunk
            context = await self._browser.new_context()
            await context.tracing.start(screenshots=True, snapshots=True, sources=True)
            await context.tracing.start_chunk()
            page: PlaywrightPage = await self._browser.new_page()
            await context.tracing.stop_chunk(path=os.path.join(DEBUG_FILEPATH, "_search_new_page.zip"))
            await navigate_to_google(page, context=context)
            await perform_google_search(page, query, context=context)
            return await extract_links(page, query)

        else:
            page = await self._browser.new_page()
            await navigate_to_google(page)
            await perform_google_search(page, query)
            return await extract_links(page, query)


    async def _skip_exc_search(self, query, num_results=10):
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


