

import aiohttp
import requests
from urllib.parse import ParseResult, urlparse


async def async_fetch_robots_txt(url: str) -> str:
    parsed_url: ParseResult = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    robots_url = f"{base_url}/robots.txt"

    async with aiohttp.ClientSession() as session:
        async with session.get(robots_url) as response:
            if response.status == 200:
                return await response.text()
            return ""


async def fetch_robots_txt(url: str) -> str:
    parsed_url: ParseResult = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    robots_url = f"{base_url}/robots.txt"
    try:
        response = requests.get(robots_url, timeout=10)
        if response.status_code == 200:
            return response.text
        return ""
    except requests.RequestException:
        return ""












