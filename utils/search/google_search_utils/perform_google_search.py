import os

from config.config import DEBUG_FILEPATH, GOOGLE_AUTOFILL_SUGGESTIONS_HTML_TAG

async def _close_autofill_suggestions(page, context=None):
    """
    Close Google autofill suggestions dropdown that may obscure search elements.
    
    Google autofill suggestions often get in the way of the search button.
    This function closes the suggestion dropdown by clicking on a specific
    element before looking for the search button. This is a workaround for
    when the "Google Search" button is obscured by the dropdown menu.
    
    Args:
        page: Playwright page object for the Google search page.
        context (optional): Playwright context for debugging traces.
    
    Returns:
        None: This function performs UI interaction side effects.
    
    Raises:
        playwright.async_api.Error: If the autofill suggestions element cannot be found or clicked.
    
    Example:
        >>> await _close_autofill_suggestions(page)
        # Closes any open autofill dropdown on the Google page
    """
    await page.locator(GOOGLE_AUTOFILL_SUGGESTIONS_HTML_TAG).click()


async def perform_google_search(page, search_query, context=None):
    """
    Fill in Google search bar with query and perform search.
    
    Enters the search query into Google's search input field, closes any
    autofill suggestions that might interfere, and clicks the search button
    to execute the search. Optionally enables debugging traces.
    
    Args:
        page: Playwright page object for the Google search page.
        search_query (str): The search query text to enter and search for.
        context (optional): Playwright context for debugging traces.
    
    Returns:
        None: This function performs search interaction side effects.
    
    Raises:
        playwright.async_api.Error: If search elements cannot be found or interacted with.
    
    Example:
        >>> await perform_google_search(page, "municipal code sales tax")
        # Fills search bar and clicks search button
        >>> await perform_google_search(page, "ordinance", context)
        # Same as above but with debugging traces enabled
    """
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
