import os

from config import DEBUG_FILEPATH, GOOGLE_DOMAIN_URL

async def navigate_to_google(page, context=None):
    """Navigate to Google domain."""
    if context: # Debug route.
        await context.tracing.start_chunk()
        await page.goto(GOOGLE_DOMAIN_URL)
        await page.wait_for_load_state("networkidle")
        await context.tracing.stop_chunk(path=os.path.join(DEBUG_FILEPATH, "_navigate_to_google.zip"))

    else:
        await page.goto(GOOGLE_DOMAIN_URL)
        await page.wait_for_load_state("networkidle")

