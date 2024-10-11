
import os

from config import GOOGLE_SEARCH_RESULT_TAG, DEBUG_FILEPATH
from logger import Logger

from utils.shared.safe_format import safe_format
from utils.shared.sanitize_filename import sanitize_filename

log_level=10
logger = Logger(logger_name=__name__, log_level=log_level)

def _check_for_empty_sublists(urls: list[list[str]]) -> bool:
    results = [
        not sublist for sublist in urls
    ]
    if False in results:
        return False 
    else:
        return True


async def extract_links(page, query: str) -> list[str] | list[None]:
    """
    Use javascript to extract URLs from a Google page.

    #### Example
    >>> urls = await extract_links(page)
    >>> for url in urls:
    >>>    logger.debug(f"Found URL: {url}")
    """
    javascript = """
        () => Array.from(document.querySelectorAll('{GOOGLE_SEARCH_RESULT_TAG}')).map(a => a.href)
    """
    args = {
        "GOOGLE_SEARCH_RESULT_TAG": GOOGLE_SEARCH_RESULT_TAG or 'div.yuRUbf > a'
    }
    javascript = safe_format(javascript, **args)
    urls = await page.evaluate(javascript)
    logger.debug(f"urls for query '{query}': {urls}")
    if log_level == 10:
        check = _check_for_empty_sublists
        if check:
            filename = sanitize_filename(query)
            path = os.path.join(DEBUG_FILEPATH, "playwright", f"{filename}.jpeg")
            await page.screenshot(path=path, type='jpeg')
    return urls

