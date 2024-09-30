import re
from urllib.parse import urlparse


def can_fetch(url: str, robot_rules: dict) -> bool:
    """
    Compare a URL to a robots.txt dictionary and see if we can scrape it.
    """
    path = urlparse(url).path

    # Check if path matches any allow rule
    for allow_path in robot_rules.get('allow', []):
        if re.match(allow_path.replace('*', '.*'), path):
            return True

    # Check if path matches any disallow rule
    for disallow_path in robot_rules.get('disallow', []):
        if re.match(disallow_path.replace('*', '.*'), path):
            return False

    # If no rules match, it's allowed by default
    return True

