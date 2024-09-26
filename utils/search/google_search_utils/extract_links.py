
from config import GOOGLE_SEARCH_RESULT_TAG
from ..google_search import logger

from utils.shared.safe_format import safe_format


async def extract_links(page) -> list[str] | list[None]:
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
    logger.debug(f"urls: {urls}\ntype(urls): {type(urls)}")
    return urls

