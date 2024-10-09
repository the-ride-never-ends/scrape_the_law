from typing import Any





def parse_robots_txt(robots_txt: str) -> dict[str,dict[str,Any]]:
    """
    Parse a robots.txt file and return it as a dictionary.
    Args;

    Example:
    >>> example_rules = {
            '*': {
                'allow': ['/public/', '/images/'],
                'disallow': ['/private/', '/admin/'],
                'crawl_delay': 5.0
            },
            'googlebot': {
                'allow': ['/news/'],
                'disallow': ['/search', '/login'],
                'crawl_delay': 2.0
            },
            'bingbot': {
                'allow': ['/blog/'],
                'disallow': ['/members/'],
                'crawl_delay': 3.5
            }}
    """
    rules = {'*': {'allow': [], 'disallow': [], 'crawl_delay': 0}}
    current_agent = '*'

    for line in robots_txt.splitlines():
        line = line.strip().lower()
        if line.startswith('user-agent:'): # 
            current_agent = line.split(':', 1)[1].strip()
            if current_agent not in rules:
                rules[current_agent] = {'allow': [], 'disallow': [], 'crawl_delay': 0}
        elif line.startswith('disallow:'):
            rules[current_agent]['disallow'].append(line.split(':', 1)[1].strip())
        elif line.startswith('allow:'):
            rules[current_agent]['allow'].append(line.split(':', 1)[1].strip())
        elif line.startswith('crawl-delay:'):
            rules[current_agent]['crawl_delay'] = float(line.split(':', 1)[1].strip())

    return rules
