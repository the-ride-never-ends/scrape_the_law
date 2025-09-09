

import aiohttp
import requests
from urllib.parse import ParseResult, urlparse

def _make_robots_txt_url(url: str) -> str:
    """
    Construct the robots.txt URL for a given website URL.
    
    Takes any URL from a website and constructs the corresponding
    robots.txt URL by extracting the scheme and netloc and appending
    '/robots.txt' to the base URL.
    
    Args:
        url (str): Any URL from the target website.
    
    Returns:
        str: The robots.txt URL for the website.
    
    Raises:
        ValueError: If the URL is malformed and cannot be parsed.
    
    Example:
        >>> _make_robots_txt_url("https://example.com/some/page")
        'https://example.com/robots.txt'
        >>> _make_robots_txt_url("http://api.site.org/v1/data")
        'http://api.site.org/robots.txt'
    """
    parsed_url: ParseResult = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return f"{base_url}/robots.txt"

async def async_fetch_robots_txt(url: str) -> str:
    """
    Asynchronously fetch the robots.txt content for a website.
    
    Downloads the robots.txt file from a website using aiohttp for
    async HTTP operations. Returns the content as a string if successful,
    or an empty string if the file doesn't exist or can't be fetched.
    
    Args:
        url (str): Any URL from the target website.
    
    Returns:
        str: The robots.txt content as a string, or empty string if
            robots.txt is not found or inaccessible.
    
    Raises:
        aiohttp.ClientError: If HTTP request fails.
        asyncio.TimeoutError: If request times out.
    
    Example:
        >>> content = await async_fetch_robots_txt("https://example.com")
        >>> if content:
        ...     print("Found robots.txt")
        >>> else:
        ...     print("No robots.txt found")
    """
    robots_url = _make_robots_txt_url(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(robots_url) as response:
            if response.status == 200:
                return await response.text()
            return ""

async def fetch_robots_txt(url: str) -> str:
    """
    Synchronously fetch the robots.txt content for a website.
    
    Downloads the robots.txt file from a website using the requests library
    for synchronous HTTP operations. Returns the content as a string if
    successful, or an empty string if the file doesn't exist or can't be fetched.
    Includes timeout and exception handling.
    
    Args:
        url (str): Any URL from the target website.
    
    Returns:
        str: The robots.txt content as a string, or empty string if
            robots.txt is not found or inaccessible.
    
    Raises:
        None: All exceptions are caught and handled gracefully by returning
            an empty string.
    
    Example:
        >>> content = fetch_robots_txt("https://example.com")
        >>> if content:
        ...     print("Found robots.txt")
        >>> else:
        ...     print("No robots.txt found or accessible")
    """
    robots_url = _make_robots_txt_url(url)
    try:
        response = requests.get(robots_url, timeout=10)
        if response.status_code == 200:
            return response.text
        return ""
    except requests.RequestException:
        return ""
