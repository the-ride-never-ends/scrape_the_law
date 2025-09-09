import os

from config.config import DEBUG_FILEPATH, GOOGLE_DOMAIN_URL

async def navigate_to_google(page, context=None):
    """
    Navigate to Google domain and wait for page to load.
    
    Navigates to the Google homepage and waits for network idle state.
    Optionally starts/stops tracing for debugging purposes when context is provided.
    
    Args:
        page: Playwright page object to navigate.
        context (optional): Playwright browser context for debugging traces.
            If provided, enables tracing around the navigation.
    
    Returns:
        None: This function performs navigation side effects.
    
    Raises:
        playwright.async_api.Error: If navigation fails or times out.
    
    Example:
        >>> async with playwright.async_api.async_playwright() as p:
        ...     browser = await p.chromium.launch()
        ...     page = await browser.new_page()
        ...     await navigate_to_google(page)
        ...     # Page is now at Google homepage
    """
    if context: # Debug route.
        await context.tracing.start_chunk()
        await page.goto(GOOGLE_DOMAIN_URL)
        await page.wait_for_load_state("networkidle")
        await context.tracing.stop_chunk(path=os.path.join(DEBUG_FILEPATH, "_navigate_to_google.zip"))

    else:
        await page.goto(GOOGLE_DOMAIN_URL)
        await page.wait_for_load_state("networkidle")

