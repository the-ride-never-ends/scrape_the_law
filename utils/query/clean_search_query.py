

def clean_search_query(query: str) -> str:
    """
    Remove leading digits from search queries to improve search engine compatibility.

    Some search tools (e.g., Google) will fail to return results if the
    query has a leading digit like "1. LangCh...". This function extracts
    the main query text after the first double quote when a leading digit
    is detected.

    Args:
        query (str): Input query that may or may not contain a leading digit.

    Returns:
        str: Cleaned query with leading digits and formatting removed.
        
    Raises:
        None
        
    Example:
        >>> clean_search_query('1. "sales tax ordinance"')
        'sales tax ordinance'
        >>> clean_search_query('sales tax ordinance')
        'sales tax ordinance'
        >>> clean_search_query('2. "municipal code" ')
        'municipal code'
        >>> clean_search_query('')
        ''
    """
    query = query.strip()
    if len(query) < 1:
        return query

    if not query[0].isdigit():
        return query.strip()

    if (first_quote_pos := query[:-1].find('"')) == -1:
        return query.strip()

    last_ind = -1 if query.endswith('"') else None

    # fmt: off
    return query[first_quote_pos + 1:last_ind].strip()
