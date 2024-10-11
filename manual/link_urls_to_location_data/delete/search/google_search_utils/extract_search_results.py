
from config import GOOGLE_SEARCH_RESULT_TAG

from utils.shared.safe_format import safe_format

async def extract_search_results(page):
    """
    Use JavaScript to extract links and text from search results.
    #### Example
    >>> search_results = await extract_search_results(page)\n
    >>> for result in search_results:\n
    >>>     logger.debug(f"Link: {result['href']}, Text: {result['text']}")
    """

    javascript = """
        () => {
            const links = Array.from(document.querySelectorAll('{GOOGLE_SEARCH_RESULT_TAG}'));
            return links.map(link => ({
                href: link.href,
                text: link.querySelector('h3')?.textContent || ''
            }));
        }
    """
    args = {
        "GOOGLE_SEARCH_RESULT_TAG": GOOGLE_SEARCH_RESULT_TAG
    }
    javascript = safe_format(javascript, **args)

    results = await page.evaluate(javascript)
    return results
