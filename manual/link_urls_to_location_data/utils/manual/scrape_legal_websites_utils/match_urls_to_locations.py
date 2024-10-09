import os
import re

from typing import NamedTuple

import pandas as pd

from config import OUTPUT_FOLDER

from logger import Logger

logger = Logger(logger_name=__name__, stacklevel=3)

def _remove_non_places(text: str):
    non_places = (
        "district", "codes", "tribe", "code", "comission", "jury", "system",  "council"
    )
    if any(non_places) in text.lower():
        return True
    else:
        return False


def _is_place_in_text(place_name: str, class_code:str,  text: str) -> bool:
    """
    NOTE Already narrowed to a single state.
    """
    # Define county and county equivalents.
    county_eqs = (
        "county", "borough", "parish", "census area", "municipality",
        "city and borough", "consolidated government", "metropolitan government",
        "unified government", "city-county",
    )

    # Make the text and place_name lower case
    text = text.lower()
    place_name = place_name.lower()
    logger.debug(f"text: {text}")
    logger.debug(f"place_name: {place_name}")

    # Remove anything before the first instance of 'of'
    clean_place_name_regex = re.compile(
        r"^.*?of\s+",
        flags=re.IGNORECASE
    )
    place_name = re.sub(clean_place_name_regex, '', place_name)
    logger.debug(f"removed everything before first 'of' place_name: {place_name}")

    # Remove anything between parentheses.
    clean_parentheses_regex = re.compile(
        r'\([^()]*\)',
        flags=re.IGNORECASE
    )
    place_name = re.sub(clean_parentheses_regex, '', place_name)
    logger.debug(f"removed anything between parentheses: {place_name}")

    # Create a regex pattern to match the place name
    pattern = r'\b' + re.escape(place_name) + r'\b'
    logger.debug(f"final placename: {place_name}")
    logger.debug(f"final regex pattern: {pattern}")

    if "h" in class_code: # Counties have to have county or a county equivalent in the name.
        return True if any(county_eqs) in text and bool(re.search(pattern, text)) else False
    else:
        return bool(re.search(pattern, text))


def _match_urls_to_locations(row: NamedTuple, site_df: pd.DataFrame) -> dict[str,str,str,str]|None:

    # Filter site_df for the current state
    state_sites_df: pd.DataFrame = site_df[site_df['state_code'] == row.state_code]

    # Check if the place name is in any of the text descriptions
    matches = state_sites_df[state_sites_df['text'].apply(lambda text: _is_place_in_text(row.place_name, row.class_code, text))]
    if not matches.empty:
        if matches == 1:
            return {
                'gnis': row.gnis,
                'href': matches.iloc[0]['href'],
                'state_code': row.state_code,
                'source': row.source
            }
        else:
            logger.warning(f"{row.place_name}, {row.class_code} returned multiple matches\n{matches}",f=True,q=False)
            logger.info("Returning None...")
            return None
    else:
        logger.info("No matches found for {row.place_name}, {row.class_code}. Returning None...",q=False)
        return None


def match_urls_to_locations(location_df: pd.DataFrame, site_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compares a dataframe of site URLs 'site_df' and locations data 'location_df' based on their place_name and text columns, respectively.
    Save matches to a return dataframe 'output_df', otherwise save it to a CSV for further review.

    Examples:
    >>> # Example site_df
    >>>     state_code                         state_url                                    href           text    source
    >>> 0           AK  https://library.municode.com/ak/  https://library.municode.com/ak/coker  Town of Coker      municode
    >>> 1           AL  https://library.municode.com/al/  https://library.municode.com/al/haines  Haines County     municode
    >>> 2           AR  https://library.municode.com/ar/  https://library.municode.com/ar/zinc             Zinc        municode
    >>> 3           AR  https://library.municode.com/ar/  https://library.municode.com/ar/ozark  Ozark Water District  municode
    >>> 4           AR  https://library.municode.com/ar/  https://library.municode.com/ar/ozark  City and Borough of St. Claire  municode
    >>> # Example location_df
    >>>        gnis       place_name  class_code  state_code
    >>> 0   2406291    Town of Coker          C1          AL
    >>> 1   1419970           Haines          H1          AK
    >>> 2   2406929     Town of Zinc          C1          AR
    >>> # Example output_df
    >>>        gnis                                      href   state_code
    >>> 0   2406291    https://library.municode.com/ak/coker            AL
    >>> 1   1419970    https://library.municode.com/al/haines           AK
    >>> 2   2406929    https://library.municode.com/ar/zinc             AR
    """

    # Filter site_df of non-places (e.g. Water District, Building Codes, etc.)
    places_df = site_df[site_df['text'].apply(lambda text: _remove_non_places(text))]
    non_places_df = site_df[~site_df['text'].apply(lambda text: _remove_non_places(text))]

    # Save the non-places to a CSV file.
    non_places_df_csv_path = os.path.join(OUTPUT_FOLDER,"non_places_df.csv")
    non_places_df.to_csv(non_places_df_csv_path, index=False)

    # Connect scraped URLs to their GNIS locations based of their link text.
    output_list = [
        result for row in location_df.itertuples() if (result := _match_urls_to_locations(row, places_df)) is not None
    ]

    # Save unmatched entries to CSV for further review
    output_df = pd.DataFrame.from_dict(output_list)
    unmatched = output_df[~output_df['gnis'].isin(location_df['gnis'])]
    if not unmatched.empty:
        non_places_df_csv_path = os.path.join(OUTPUT_FOLDER,"non_places_df.csv")
        unmatched.to_csv('unmatched_urls.csv', index=False)

    return output_df
