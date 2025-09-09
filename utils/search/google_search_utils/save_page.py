
from playwright.async_api import async_playwright


def save_mhtml(path: str, text: str):
    """
    Save MHTML content to a file.
    
    Writes MHTML (MIME HTML) content to the specified file path with UTF-8 encoding.
    MHTML format preserves complete web pages including embedded resources.
    
    Args:
        path (str): File path where MHTML content should be saved.
        text (str): MHTML content to write to file.
    
    Returns:
        None: This function performs file I/O side effects.
    
    Raises:
        IOError: If file cannot be written to the specified path.
        UnicodeEncodeError: If text cannot be encoded as UTF-8.
    
    Example:
        >>> mhtml_content = "From: <Saved by Blink>\nSnapshot-Content-Location: ..."
        >>> save_mhtml("/tmp/page.mhtml", mhtml_content)
        # Saves MHTML content to file
    """
    with open(path, mode='w', encoding='UTF-8', newline='\n') as file:
        file.write(text)


def save_page(url: str, path: str):
    """
    Capture and save a web page as MHTML format.
    
    Launches a Chromium browser, navigates to the specified URL, captures
    the page as MHTML (complete with embedded resources), and saves it to disk.
    
    Args:
        url (str): URL of the web page to capture and save.
        path (str): File path where the MHTML file should be saved.
    
    Returns:
        None: This function performs web scraping and file I/O side effects.
    
    Raises:
        playwright.async_api.Error: If browser launch or navigation fails.
        IOError: If MHTML file cannot be saved to the specified path.
    
    Example:
        >>> save_page("https://example.com", "/tmp/example.mhtml")
        # Captures the page and saves as MHTML file
        >>> save_page("https://google.com", "google_homepage.mhtml")
        # Saves Google homepage with all embedded resources
    """
    with async_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url)

        client = page.context.new_cdp_session(page)
        mhtml = client.send("Page.captureSnapshot")['data']
        save_mhtml(path, mhtml)
        browser.close()

        