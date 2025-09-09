from config.config import LEGAL_WEBSITE_DICT, US_STATE_CODES

def make_urls(source: str) -> list[str]:
    """
    Make urls function.
    
    TODO: Add proper description.
    
    Args:
        source (str): Description needed.
    
    Returns:
        Description needed.
    
    Raises:
        TODO: Document exceptions.
    
    Example:
        >>> # TODO: Add usage example
        >>> make_urls()
    """
    url = LEGAL_WEBSITE_DICT[source]["base_url"]
    return [
        f"{url}{code if source == 'general_code' else code.lower()}" 
        for code in US_STATE_CODES
    ]