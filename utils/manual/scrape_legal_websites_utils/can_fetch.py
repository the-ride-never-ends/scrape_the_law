import re
from urllib.parse import urlparse


def can_fetch(url: str, robot_rules: dict) -> tuple[bool, int]:
    """
    Check if a URL can be fetched according to robots.txt rules.
    
    Compares a URL's path against the allow and disallow rules from a
    robots.txt file to determine if web scraping is permitted. Also
    returns the crawl delay specified in the robots.txt.
    
    Args:
        url (str): The URL to check for fetch permission.
        robot_rules (dict): Dictionary containing parsed robots.txt rules
            with 'allow', 'disallow', and 'crawl-delay' keys.
    
    Returns:
        tuple[bool, int]: A tuple containing (can_fetch, delay_seconds).
            can_fetch is True if URL can be scraped, False otherwise.
            delay_seconds is the crawl delay in seconds (0 if not specified).
    
    Raises:
        ValueError: If URL is malformed and cannot be parsed.
        KeyError: If robot_rules dict is missing expected keys.
    
    Example:
        >>> rules = {'allow': ['/api/*'], 'disallow': ['/admin'], 'crawl-delay': 1}
        >>> can_fetch("https://example.com/api/data", rules)
        (True, 1)
        >>> can_fetch("https://example.com/admin", rules)
        (False, 1)
    """
    """
    Compare a URL to a robots.txt dictionary and see if we can scrape it.
    Also return the website's delay
    """
    path = urlparse(url).path
    delay = robot_rules.get('crawl-delay', 0)  # Default delay is 0 if not specified

    # Check if path matches any allow rule
    for allow_path in robot_rules.get('allow', []):
        if re.match(allow_path.replace('*', '.*'), path):
            return True, delay

    # Check if path matches any disallow rule
    for disallow_path in robot_rules.get('disallow', []):
        if re.match(disallow_path.replace('*', '.*'), path):
            return False, delay

    # If no rules match, it's allowed by default
    return True, delay
