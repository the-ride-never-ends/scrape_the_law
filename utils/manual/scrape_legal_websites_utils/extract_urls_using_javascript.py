
import os
from typing import Any

from config.config import GOOGLE_SEARCH_RESULT_TAG, DEBUG_FILEPATH, LEGAL_WEBSITE_DICT
from logger.logger import Logger

from utils.shared.safe_format import safe_format
from utils.shared.sanitize_filename import sanitize_filename

from playwright.async_api import Page as AsyncPlaywrightPage
from playwright.sync_api import Page as PlaywrightPage


log_level=10
logger = Logger(logger_name=__name__, log_level=log_level)

def _check_for_empty_sublists(urls: list[dict[str, str]]) -> bool:
    results = [
        not bool(dictionary) for dictionary in urls
    ]
    return all(results)

def safe_format_js_selector(source: str, js_command: str=None, args: dict[str,Any]=None) -> str:
    """"
    Safe format a JavaScript string with input arguments.
    Defaults to 'querySelectorAll'
    
    Example:
    >>> # Extract all URLs with class "codeLink"
    >>> source = "www.example.com"
    >>> args = {'TARGET': 'codeLink'}
    >>> js_command = '''
    >>>  () => Array.from(document.querySelectorAll('a.{TARGET}')).map(a => ({
    >>>     href: a.href,
    >>>     text: a.textContent.trim()
    >>> }))
    >>> '''
    >>> return safe_format_js_selector(source, js_command=js_command, args=args)
    [{href: www.example.com/example_href, text: 'example text'}]
    """
    javascript = js_command or """
        () => Array.from(document.querySelectorAll('a.{TARGET}')).map(a => ({
            href: a.href,
            text: a.textContent.trim()
        }))
    """
    args = args or {
        "TARGET": LEGAL_WEBSITE_DICT[source]["target_class"]
    }
    return safe_format(javascript, **args)



async def extract_urls_using_javascript(page: PlaywrightPage, source: str) -> list[dict[str,str]] | list[None]:
    """
    Use javascript to extract URLs and associated text from a webpage.

    Example:
    >>> urls = extract_urls_using_javascript(page, source)
    >>> for url, text in urls_dict.values:
    >>>    logger.debug(f"Found URL: {url} txt: {text}")
    """
    javascript = safe_format_js_selector(source)
    urls_dict: list[dict] = page.evaluate(javascript)
    logger.debug(f"urls for url '{page.url}': {urls_dict}")
    if log_level == 10:
        check = _check_for_empty_sublists()
        if check:
            filename = sanitize_filename(page.url)
            path = os.path.join(DEBUG_FILEPATH, "playwright", f"{filename}.jpeg")
            page.screenshot(path=path, type='jpeg')
    return urls_dict


async def async_extract_urls_using_javascript(page: AsyncPlaywrightPage, source: str) -> list[dict[str,str]] | list[None]:
    """
    Use javascript to asynchronously extract URLs and associated text from a webpage.

    Example:
    >>> urls = extract_urls_using_javascript(page, source)
    >>> for url, text in urls_dict.values:
    >>>    logger.debug(f"Found URL: {url} txt: {text}")
    """
    # Format the js code with
    javascript = safe_format_js_selector(source)

    # Get the URLs
    urls_dict_list: list[dict] = await page.evaluate(javascript)
    logger.debug(f"urls for url '{page.url}': {urls_dict_list}")

    # If in debug, check for empty lists. 
    # If there are any, take a screenshot.
    if log_level == 10: 
        check = _check_for_empty_sublists(urls_dict_list)
        if check:
            filename = sanitize_filename(page.url)
            path = os.path.join(DEBUG_FILEPATH, "playwright", f"{filename}.jpeg")
            await page.screenshot(path=path, type='jpeg')

    return urls_dict_list

