
import os

from config import GOOGLE_SEARCH_RESULT_TAG, DEBUG_FILEPATH, LEGAL_WEBSITE_DICT
from logger import Logger

from utils.shared.safe_format import safe_format
from utils.shared.sanitize_filename import sanitize_filename

from playwright.async_api import Page as AsyncPlaywrightPage

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


# # Extract all links with class "codeLink"
# links = page.evaluate('''
#     () => Array.from(document.querySelectorAll('a.codeLink')).map(a => ({
#         href: a.href,
#         text: a.textContent.trim()
#     }))
# ''')


async def extract_urls_using_javascript(page: AsyncPlaywrightPage, source: str) -> list[str] | list[None]:
    """
    Use javascript to extract URLs and associated text from a webpage.

    Example:
    >>> urls = await extract_links(page)
    >>> for url, text in urls_dict.values:
    >>>    logger.debug(f"Found URL: {url} txt: {text}")
    """
    javascript = """
        () => Array.from(document.querySelectorAll('a.{TARGET}')).map(a => ({
            href: a.href,
            text: a.textContent.trim()
        }))
    """
    args = {
        "TARGET": LEGAL_WEBSITE_DICT[source]["target_class"]
    }
    javascript = safe_format(javascript, **args)
    urls_dict: list[dict] = await page.evaluate(javascript)
    logger.debug(f"urls for url '{page.url}': {urls_dict}")
    if log_level == 10:
        check = _check_for_empty_sublists
        if check:
            filename = sanitize_filename(page.url)
            path = os.path.join(DEBUG_FILEPATH, "playwright", f"{filename}.jpeg")
            await page.screenshot(path=path, type='jpeg')
    return urls_dict

