import os

from config import DEBUG_FILEPATH, GOOGLE_AUTOFILL_SUGGESTIONS_HTML_TAG

async def _close_autofill_suggestions(page, context=None):
    """Google autofill suggestions often get in way of search button.

    We get around this by closing the suggestion dropdown before
    looking for the search button. Looking for the "Google Search"
    button doesn't work because it is sometimes obscured by the dropdown
    menu. Clicking the "Google" logo can also fail when they add
    seasonal links/images (e.g. holiday logos). Current solutions is to
    look for a specific div at the top of the page.
    TODO This is VERY hacky. It should probably be changed to make it more robust.
    """
    await page.locator(GOOGLE_AUTOFILL_SUGGESTIONS_HTML_TAG).click()


async def perform_google_search(page, search_query, context=None):
    """Fill in search bar with user query and click search button"""
    if context: # Debug route
        await context.tracing.start_chunk()
        await page.get_by_label("Search", exact=True).fill(search_query)
        await _close_autofill_suggestions(page, context=None)
        await page.get_by_role("button", name="Google Search").click()
        await context.tracing.stop_chunk(path=os.path.join(DEBUG_FILEPATH, "perform_google_search.zip"))

    else:
        await page.get_by_label("Search", exact=True).fill(search_query)
        await _close_autofill_suggestions(page, context=None)
        await page.get_by_role("button", name="Google Search").click()