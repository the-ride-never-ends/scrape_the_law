import re

from typing import NamedTuple

import pandas as pd

def connect_scraped_urls_to_locations(location_df: pd.DataFrame, site_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compares a dataframe of site URLs 'site_df' and locations data 'location_df' based on their place_name and text columns, respectively.
    Save matches to a return dataframe 'output_df', otherwise save it to a CSV for further review.

    Examples:
    >>> # Example site_df
    >>>     state_code                         state_url                                    href           text
    >>> 0           AK  https://library.municode.com/ak/  https://library.municode.com/ak/coker  Town of Coker
    >>> 1           AL  https://library.municode.com/al/  https://library.municode.com/al/haines  Haines County
    >>> 2           AR  https://library.municode.com/ar/  https://library.municode.com/ar/zinc             Zinc
    >>> 3           AR  https://library.municode.com/ar/  https://library.municode.com/ar/ozark  Ozark Water District
    >>> 4           AR  https://library.municode.com/ar/  https://library.municode.com/ar/ozark  City and Borough of St. Claire
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

    def is_place_in_text(place_name: str, text: str) -> bool:
        # Make the text lower case
        text = text.lower()
        place_name = place_name.lower()

        # Remove common words like "City of", "Town of", etc.
        clean_place_name_regex = re.compile(
            r"^(City|City and Borough|Town|Borough|Village) of\s+",
            flags=re.IGNORECASE
        )
        clean_place_name = re.sub(clean_place_name_regex, '', place_name)
        # Create a regex pattern to match the place name
        pattern = r'\b' + re.escape(clean_place_name) + r'\b'
        
        # Check if the pattern is in the text
        return bool(re.search(pattern, text))

    def process_location(row: pd.Series, site_df: pd.DataFrame) -> pd.Series:
        place_name = row['place_name']
        state_code = row['state_code']
        
        # Filter site_df for the current state
        state_sites = site_df[site_df['state_code'] == state_code]

        # Check if the place name is in any of the text descriptions
        matches = state_sites[state_sites['text'].apply(lambda x: is_place_in_text(place_name, x))]
        if not matches.empty:
            return pd.Series({
                'gnis': row['gnis'],
                'href': matches.iloc[0]['href'],
                'state_code': state_code
            })
        return None

    output_df = pd.DataFrame(columns=['gnis', 'href', 'state_code'])
    for _, row in location_df.iterrows():
        result = process_location(row, site_df)
        if result is not None:
            output_df = output_df.append(result, ignore_index=True)

    # Save unmatched entries to CSV for further review
    unmatched = location_df[~location_df['gnis'].isin(output_df['gnis'])]
    if not unmatched.empty:
        unmatched.to_csv('unmatched_locations.csv', index=False)

    return output_df
