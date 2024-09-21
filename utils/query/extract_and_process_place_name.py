
import re


def extract_place_name(full_name: str) -> str:
    """
    Extracts the place name from a full name string.

    Examples:
        >>> extract_place_name("Town of Springfield")
        "Springfield"
        >>> extract_place_name("New York City")
        "New York City"
    """
    pattern = r'^(?:Town|City|Village|Borough)\s+of\s+(.+)$'
    match = re.search(pattern, full_name)
    if match:
        return match.group(1)
    return full_name  # Return the full name if it doesn't follow the pattern


def process_place_name(name: str) -> str:
    """
    Processes a place name by converting it to lowercase and removing special characters and whitespace.

    Examples:
        >>> process_place_name("New York City")
        "newyorkcity"
        >>> process_place_name("St. Louis")
        "stlouis"
    """
    if name is None:
        return None
    # Convert to lowercase
    name = name.lower()
    # Remove special characters and whitespace
    name = re.sub(r'[^a-z0-9]', '', name)
    return name


def extract_and_process_place_name(full_name: str) -> str:
    """
    Example:
        >>> extract_and_process_place_name("Town of Springfield")
        "springfield"
    """
    extracted = extract_place_name(full_name)
    return process_place_name(extracted)