
def sanitize_filename(input: str) -> str:
    """
    Sanitize a string to be used as (part of) a filename.
    
    Removes or replaces characters that are not allowed in filenames on most
    operating systems. Replaces disallowed characters with dots and consolidates
    multiple consecutive dots into single dots.
    
    Args:
        input (str): The string to sanitize for use as a filename.
    
    Returns:
        str: A sanitized string safe for use as a filename.
    
    Raises:
        None
    
    Example:
        >>> sanitize_filename('my/file:name.txt')
        'my.file.name.txt'
        >>> sanitize_filename('file<with>bad*chars')
        'file.with.bad.chars'
        >>> sanitize_filename('multiple...dots')
        'multiple.dots'
    """
    disallowed = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in disallowed:
        input = input.replace(char, ".")
    input = '.'.join(filter(None, input.split('.')))
    return input
