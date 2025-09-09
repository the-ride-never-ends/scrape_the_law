

import aiohttp
import requests
from urllib.parse import ParseResult, urlparse

def _make_robots_txt_url(url: str) -> str:
    """
     make robots txt url function.
    
    TODO: Add proper description.
    
    Args:
        url (str): Description needed.
    
    Returns:
        str: Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> _make_robots_txt_url()
    """
    parsed_url: ParseResult = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return f"{base_url}/robots.txt"

async def async_fetch_robots_txt(url: str) -> str:
    """
    Async fetch robots txt function.
    
    TODO: Add proper description.
    
    Args:
        url (str): Description needed.
    
    Returns:
        str: Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> async_fetch_robots_txt()
    """
    robots_url = _make_robots_txt_url(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(robots_url) as response:
            if response.status == 200:
                return await response.text()
            return ""

async def fetch_robots_txt(url: str) -> str:
    """
    Fetch robots txt function.
    
    TODO: Add proper description.
    
    Args:
        url (str): Description needed.
    
    Returns:
        str: Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> fetch_robots_txt()
    """
    robots_url = _make_robots_txt_url(url)
    try:
        response = requests.get(robots_url, timeout=10)
        if response.status_code == 200:
            return response.text
        return ""
    except requests.RequestException:
        return ""
