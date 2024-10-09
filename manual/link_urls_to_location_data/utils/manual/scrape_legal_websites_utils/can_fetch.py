import re
from urllib.parse import urlparse


def can_fetch(url: str, robot_rules: dict) -> tuple[bool, int]:
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
