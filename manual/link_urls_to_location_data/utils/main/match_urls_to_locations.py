# """


# Example Usage:
#     >>> matchmaker = Matcher(sources_df, locations_df)
#     >>> output_df = matchmaker.match()
# """
# import os
# import re
# import time
# import traceback
# from typing import NamedTuple


# import pandas as pd


# from config import OUTPUT_FOLDER
# from logger import Logger
# logger = Logger(logger_name=__name__, stacklevel=2)


# def _make_county_boolean(locations_df: pd.DataFrame) -> pd.DataFrame:
#     # Rework the class_code in locations_df to a be a county boolean.s
#     locations_df = locations_df.rename(columns={'class_code': 'county'})
#     locations_df['county'] = locations_df['county'].str.contains('H')
#     logger.debug(f"locations_df: {locations_df.columns}")
#     return locations_df


# def _save_to_csv(df: pd.DataFrame, name: str) -> None:
#     try:
#         if not name.endswith(".csv"):
#             logger.error(f"The specified name does not have a '.csv' extension")
#             raise ValueError("The specified name does not have a '.csv' extension")

#         logger.info(f"{len(df)} places were in {name.split(".")[0]}")

#         csv_path = os.path.join(OUTPUT_FOLDER, name)
#         df.to_csv(csv_path, index=False)
#         logger.info(f"Saved {name} to '{csv_path}'")

#     except Exception as e:
#         logger.debug(f"Could not save {name} to CSV: {e}")


# class Matcher:
#     """
#     Example Usage:
#         >>> matchmaker = Matcher(sources_df, locations_df)
#         >>> output_df = matchmaker.match()
#     """

#     def __init__(self, 
#                  sources_df: pd.DataFrame, 
#                  locations_df: pd.DataFrame
#                 ):
#         self._sources_df: pd.DataFrame = sources_df
#         self._locations_df: pd.DataFrame = _make_county_boolean(locations_df)
#         self.regex = regex = {
#                 'clean_of_from_name_regex': { # Remove anything before the first instance of 'of'
#                     'regex': re.compile(r"^.*?of\s+", flags=re.IGNORECASE),
#                     'operation': lambda name: re.sub(regex['clean_of_from_name_regex']['regex'], '', name)
#                 },
#                 'clean_parentheses_regex': { # Remove anything between parentheses.
#                     'regex': re.compile(r'\([^()]*\)', flags=re.IGNORECASE),
#                     'operation': lambda name: re.sub(regex['clean_parentheses_regex']['regex'], '', name)
#                 },
#                 'clean_quotation_comma_regex': { # If there's double quotation marks with a comma, remove the quotation marks and everything after the comma.
#                     'regex': re.compile(r'^"([^"]*)".*$', flags=re.IGNORECASE),
#                     'operation': lambda name: re.sub(regex['clean_quotation_comma_regex']['regex'], r'\1', name)
#                 },
#                 'clean_township_regex': { # Remove everything after 'Township', 'Charter Township', or 'Chrtr Township'
#                     'regex': re.compile(r'(Township|Charter Township|Chrtr Township|Metro Township).*$', flags=re.IGNORECASE),
#                     'operation': lambda name: re.sub(regex['clean_township_regex']['regex'], r'\1', name)
#                 }
#             }
#         self.not_places: list[str] = (
#             "district", "tribe", "code", "commission", "jury", "system",  "council",
#             "association", "corporation", "authority", "civil service",
#             "university of", "air park", "examiner", # Misc. name things, mainly for courts and trusts.
#             "children's", "rules and appeals", "clerk of court", 
#             "seminole nation", "prairie band potawatomi nation", "samish indian nation", "osage nation" # Indian tribes
#         )
#         self.county_eqs: list[str] = (
#             "county", "county_", "_county", "_county_",
#             "borough", "borough_", "_borough", "_borough_",
#             "parish", "parish_", "_parish", "_parish_",
#             "census area", "census_area", "_census_area", "_census_area_",
#             "municipality", "municipality_", "_municipality", "_municipality_",
#             "city and borough", "city_and_borough", "_city_and_borough", "_city_and_borough_",
#             "consolidated government", "consolidated_government", "_consolidated_government", "_consolidated_government_",
#             "metropolitan government", "metropolitan_government", "_metropolitan_government", "_metropolitan_government_",
#             "unified government", "unified_government", "_unified_government", "_unified_government_",
#             "city-county", "city_county", "_city_county", "_city_county_",
#         )
#         is_place = ~self._sources_df['text'].apply(self._remove_non_places)
#         self.df: dict[str, pd.DataFrame] = {
#             "locations": self._locations_df,
#             "counties": self._locations_df[self._locations_df["county"] == True],  # Split up the data into cities and counties.
#             "cities": self._locations_df[self._locations_df["county"] == False],
#             "sources": self._sources_df,
#             "places": self._sources_df[is_place], # Filter sources_df of non-places (e.g. Water District, Building Codes, etc.)
#             "non_places": self._sources_df[~is_place], # NOTE This is not perfect, but the cities are few enough they can be manually inserted into the database.
#         }
#         # Divide places into counties and cities based on regexing their URLs and text.
#         mask = self.df['places'].apply(lambda row: self._check_if_county(row), axis=1)
#         self.df['s_counties'] = self.df['places'][mask]
#         self.df['s_cities'] = self.df['places'][~mask]

#         for key, value in self.df.items():
#             logger.debug(f"{key}\n{value}")


#     def _remove_non_places(self, text: str) -> bool:
#         """
#         Filter out non-places (e.g. Water Districts, Building Codes, etc.)
#         """
#         for non_place in self.not_places:
#             if non_place in text.lower():
#                 #logger.debug(f"non_place: {non_place}\ntext: {text.lower()}")
#                 return True
#             else:
#                 continue
#         return False


#     def _check_if_county(self, row: NamedTuple) -> bool:
#         """
#         Check a row's text and href to determine if the link is to a county or a city.
#         """
#         lower_href = row.href.lower()
#         lower_text = row.text.lower()
#         return any(county in lower_href or county in lower_text for county in self.county_eqs)


#     def _is_place_in_text(self, row: NamedTuple, text: str) -> bool:
#         """
#         Check if a given place name is present in the provided text, considering various formatting rules and special cases.

#         Args:
#             place_name (str): The name of the place to search for.
#             class_code (str): A code indicating the class of the place (e.g., 'h' for county or county equivalent).
#             text (str): The text to search in.

#         Returns:
#             bool: True if the place name is found in the text (considering the rules), False otherwise.

#         Examples:
#             >>> _is_place_in_text("City of Springfield", "c1", "Welcome to Springfield")
#             True
#             >>> _is_place_in_text("Franklin County", "h1", "Franklin County, Ohio")
#             True
#             >>> _is_place_in_text("Washington Township", "t1", "Washington Charter Township")
#             True
#         """
#         text = text.lower()
#         place_name = row.place_name.lower()
#         #logger.debug(f"text: {text}",off=True)
#         #logger.debug(f"place_name: {place_name}",t=0.5,off=True)
    
#         for operation in self.regex.values():
#             place_name = operation['operation'](place_name)

#         # Create a regex pattern to match the place name
#         pattern = r'\b' + re.escape(place_name) + r'\b'

#         match = bool(re.search(pattern, text))
#         if match:
#             logger.debug(f"'{place_name}' in '{text}' with class_code {row.class_code} using pattern '{pattern}'",t=2,off=True)
#         return match


#     def _match_urls_to_locations(self, row: NamedTuple) -> dict[str,str|list|None]:
#         """
#         Match an href URL to a place in sources_df based on whether the place's name is in the href's associated text or href.
#         Args:
#             row (NamedTuple): A named tuple containing information about a location, including gnis, place_name, and state_code.

#         Returns:
#             dict[str,str|list|None]: A dictionary containing the matching information:
#                 - 'gnis': The place's GNIS identifier.
#                 - 'place_name': The place's legal name.
#                 - 'state_code': The place's state code.
#                 - 'href': The matched URL(s) or None if no match is found.
#                 - 'source': The source(s) of the matched URL(s) or None if no match is found.

#         Raises:
#             Exception: If an unexpected error occurs during the matching process.

#         Note:
#             This method uses the _is_place_in_text method to check for matches in both the 'text' and 'href' columns of the state_places DataFrame.
#         """
#         logger.debug(f"row: {row}",f=True,off=True)
#         #logger.debug(f"state_specific_places_df: {state_specific_places_df.head(1)}",t=5)

#         # Check if the place name is in any of the text descriptions or hrefs
#         try:
#             text_mask = self.df['state_places'].apply(lambda x: self._is_place_in_text(row, x['text']))
#             href_mask = self.df['state_places'].apply(lambda x: self._is_place_in_text(row, x['href']))
#             matches_df = self.df['state_places'][text_mask or href_mask]

#             output_dict = {
#                 'gnis': row.gnis,
#                 'place_name': row.place_name,
#                 'state_code': row.state_code,
#             }
#             if not matches_df.empty: # NOTE Doing this in each dictionary allows us to filter later on based on "source"'s Python type. Neat!
#                 if len(matches_df) == 1:
#                     logger.debug(f"Found match for {row.place_name}",off=True)
#                     output_dict['href'] = matches_df.iloc[0]['href']
#                     output_dict['source'] = matches_df.iloc[0]['source']
#                 else:
#                     logger.debug(f"Found multiple matches for {row.place_name}",off=True)
#                     logger.debug(f"matches_df: {matches_df.head()}",t=5,off=True)
#                     output_dict['href'] = matches_df['href'].to_list()
#                     output_dict['source'] = matches_df['source'].to_list()
#             else:
#                 logger.debug(f"Found no match for {row.place_name}",off=True)
#                 output_dict['href'] = None
#                 output_dict['source'] = None
#             logger.debug(f"output_dict: {output_dict}",t=1,off=True)
#             return output_dict
#         except Exception as e:
#             logger.exception(f"Unknown exception: {e}")
#             traceback.print_exc()
#             raise e


#     def match_urls_to_locations(self) -> pd.DataFrame:
#         """
#         Compares a dataframe of site URLs 'sources_df' and locations data 'locations_df' based on their place_name and text columns, respectively.
#         Save matches to a return dataframe 'output_df', otherwise save it to a CSV for further review.

#         Examples:
#         >>> # Example sources_df
#         >>>     state_code                         state_url                                    href           text    source
#         >>> 0           AK  https://library.municode.com/ak/  https://library.municode.com/ak/coker  Town of Coker      municode
#         >>> 1           AL  https://library.municode.com/al/  https://library.municode.com/al/haines  Haines County     municode
#         >>> 2           AR  https://library.municode.com/ar/  https://library.municode.com/ar/zinc             Zinc        municode
#         >>> 3           AR  https://library.municode.com/ar/  https://library.municode.com/ar/ozark  Ozark Water District  municode
#         >>> 4           AR  https://library.municode.com/ar/  https://library.municode.com/ar/ozark  City and Borough of St. Claire  municode
#         >>> # Example locations_df
#         >>>        gnis       place_name  class_code  state_code
#         >>> 0   2406291    Town of Coker          C1          AL
#         >>> 1   1419970           Haines          H1          AK
#         >>> 2   2406929     Town of Zinc          C1          AR
#         >>> # Example output_df
#         >>>        gnis                                      href   state_code  source
#         >>> 0   2406291    https://library.municode.com/ak/coker            AL  municode
#         >>> 1   1419970    https://library.municode.com/al/haines           AK  municode
#         >>> 2   2406929    https://library.municode.com/ar/zinc             AR  municode
#         """

#         # logger.debug(f"places_df: {places_df.head()}")
#         # logger.debug(f"len(places_df): {len(places_df)}")
#         # logger.debug(f"non_places_df: {non_places_df.head()}")
#         # logger.debug(f"len(non_places_df): {len(non_places_df)}")

#         # Connect scraped URLs to their GNIS locations based on their link and text and save it to output_list
#         start = time.time()
#         locations: list[pd.DataFrame] = [
#             ("cities",self.df['cities']),
#             ("counties",self.df['counties']),
#         ]
#         output_list = []

#         for gov_type, gov_unit in locations: # This should create two routes for cities and counties.
#             gov_unit_list = []
#             for state, state_df in gov_unit.groupby("state_code"):
#                 logger.info(f"Processing places in {state}")

#                 # Select only-counties or cities from sources_df depending on which dataframe is running through it at the moment.
#                 if gov_type == "counties":
#                     input_df = self.df['s_counties']
#                 else:
#                     input_df = self.df['s_cities']

#                 # Select rows from places_df that match the grouped df's state_code
#                 state_mask = input_df['state_code'] == state
#                 state_df: pd.DataFrame = input_df[state_mask]
#                 logger.debug(f"state_df: {state_df.head()}")

#                 # Perform the actual matching.
#                 _output_list = [
#                     self._match_urls_to_locations(row) for row in state_df.itertuples()
#                 ] # -> list[dict]
#                 failed_to_match = sum(1 for result in _output_list if result['href'] is None)


#                 logger.info(f"Failed to match {failed_to_match} out of {len(state_df)} places in {state}")
#                 gov_unit_list.extend(_output_list)
#             output_list.extend(gov_unit_list)

#             # OG was 122 seconds, new one is 80 seconds.
#         logger.info(f"_match_urls_locations took {time.time() - start} seconds to run and matched {len(output_list)} places to an href",t=5) 


#         # Create DataFrames from output_list and separate into matched, unmatched, and multiple matches dataframes.
#         self.df['output_df'] = output_df = pd.DataFrame.from_dict(output_list)
    
#         self.df['matched_df'] = matched_df = output_df[output_df['source'].apply(lambda x: isinstance(x, str))]
#         self.df['unmatched_df'] = output_df[output_df['source'].apply(lambda x: True if x is None else False)]

#         self.df["multiple_sources_df"] = output_df[output_df['source'].apply(lambda x: isinstance(x, list) and len(x) > 1 and len(set(x)) == len(x))]
#         self.df["multiple_matches_df"] = output_df[output_df['source'].apply(lambda x: isinstance(x, list) and len(x) > 1 and len(set(x)) < len(x))]

#         # Save matched, unmatched, and multiple matched entries to separate CSV's for further review.
#         for name, df in self.df.items():
#             _save_to_csv(df, f"{name}.csv")

#         return matched_df
"""
Usage
    matchmaker = Matcher(sources_df, locations_df)
    output_df = matchmaker.match()
"""

import os
import re
import time
from typing import Any, NamedTuple

import pandas as pd

from config import OUTPUT_FOLDER
from logger import Logger

logger = Logger(logger_name=__name__, stacklevel=2)

class Matcher:
    def __init__(self, sources_df: pd.DataFrame, locations_df: pd.DataFrame):
        self._sources_df = sources_df
        self._locations_df = self._make_county_boolean(locations_df)
        self.regex = self._compile_regex()
        self.not_places = self._define_not_places()
        self.county_eqs = self._define_county_equivalents()
        self.df = self._prepare_dataframes()

    @staticmethod
    def _make_county_boolean(locations_df: pd.DataFrame) -> pd.DataFrame:
        locations_df = locations_df.rename(columns={'class_code': 'county'})
        locations_df['county'] = locations_df['county'].str.contains('H')
        return locations_df

    @staticmethod
    def _compile_regex() -> dict[str, dict[str, Any]]:
        return {
            'clean_of_from_name_regex': {
                'regex': re.compile(r"^.*?of\s+", flags=re.IGNORECASE),
                'operation': lambda name: re.sub(r"^.*?of\s+", '', name, flags=re.IGNORECASE)
            },
            'clean_parentheses_regex': {
                'regex': re.compile(r'\([^()]*\)', flags=re.IGNORECASE),
                'operation': lambda name: re.sub(r'\([^()]*\)', '', name, flags=re.IGNORECASE)
            },
            'clean_quotation_comma_regex': {
                'regex': re.compile(r'^"([^"]*)".*$', flags=re.IGNORECASE),
                'operation': lambda name: re.sub(r'^"([^"]*)".*$', r'\1', name, flags=re.IGNORECASE)
            },
            'clean_township_regex': {
                'regex': re.compile(r'(Township|Charter Township|Chrtr Township|Metro Township).*$', flags=re.IGNORECASE),
                'operation': lambda name: re.sub(r'(Township|Charter Township|Chrtr Township|Metro Township).*$', r'\1', name, flags=re.IGNORECASE)
            }
        }

    @staticmethod
    def _define_not_places() -> list[str]:
        return [
            "district", "tribe", "code", "commission", "jury", "system", "council",
            "association", "corporation", "authority", "civil service",
            "university of", "air park", "examiner",
            "children's", "rules and appeals", "clerk of court",
            "seminole nation", "prairie band potawatomi nation", "samish indian nation", "osage nation"
        ]

    @staticmethod
    def _define_county_equivalents() -> list[str]:
        return [
            "county", "county_", "_county", "_county_",
            "borough", "borough_", "_borough", "_borough_",
            "parish", "parish_", "_parish", "_parish_",
            "census area", "census_area", "_census_area", "_census_area_",
            "municipality", "municipality_", "_municipality", "_municipality_",
            "city and borough", "city_and_borough", "_city_and_borough", "_city_and_borough_",
            "consolidated government", "consolidated_government", "_consolidated_government", "_consolidated_government_",
            "metropolitan government", "metropolitan_government", "_metropolitan_government", "_metropolitan_government_",
            "unified government", "unified_government", "_unified_government", "_unified_government_",
            "city-county", "city_county", "_city_county", "_city_county_",
        ]

    def _prepare_dataframes(self) -> dict[str, pd.DataFrame]:
        is_place = ~self._sources_df['text'].apply(self._remove_non_places)
        df = {
            "locations": self._locations_df,
            "counties": self._locations_df[self._locations_df["county"]],
            "cities": self._locations_df[~self._locations_df["county"]],
            "sources": self._sources_df,
            "places": self._sources_df[is_place],
            "non_places": self._sources_df[~is_place],
        }
        mask = df['places'].apply(self._check_if_county, axis=1)
        df['s_counties'] = df['places'][mask]
        df['s_cities'] = df['places'][~mask]
        return df

    def _remove_non_places(self, text: str) -> bool:
        return any(non_place in text.lower() for non_place in self.not_places)

    def _check_if_county(self, row: NamedTuple) -> bool:
        lower_href = row.href.lower()
        lower_text = row.text.lower()
        return any(county in lower_href or county in lower_text for county in self.county_eqs)

    def _is_place_in_text(self, row: NamedTuple, text: str) -> bool:
        text = text.lower()
        place_name = row.place_name.lower()

        for operation in self.regex.values():
            place_name = operation['operation'](place_name)

        pattern = r'\b' + re.escape(place_name) + r'\b'
        match = bool(re.search(pattern, text))
        if match:
            logger.debug(f"'{place_name}' in '{text}' with county={row.county} using pattern '{pattern}'", t=2, off=True)
        return match

    def _match_urls_to_locations(self, row: NamedTuple, state_places: pd.DataFrame) -> dict[str, Any]:
        # logger.debug(f"row: {row}")
        try:
            text_mask = state_places.apply(lambda x: self._is_place_in_text(row, x['text']), axis=1)
            href_mask = state_places.apply(lambda x: self._is_place_in_text(row, x['href']), axis=1)
            matches_df = state_places[text_mask | href_mask]

            output_dict = {
                'gnis': row.gnis,
                'place_name': row.place_name,
                'state_code': row.state_code,
                'href': None,
                'source': None
            }

            if not matches_df.empty:
                if len(matches_df) == 1:
                    output_dict['href'] = matches_df.iloc[0]['href']
                    output_dict['source'] = matches_df.iloc[0]['source']
                else:
                    output_dict['href'] = matches_df['href'].tolist()
                    output_dict['source'] = matches_df['source'].tolist()

            return output_dict
        except Exception as e:
            logger.exception(f"Unknown exception in _match_urls_to_locations: {e}")
            raise

    def match(self) -> pd.DataFrame:
        start = time.time()
        output_list = []

        for gov_type, gov_unit in [("cities", self.df['cities']), ("counties", self.df['counties'])]:
            for state, state_df in gov_unit.groupby("state_code"):
                logger.info(f"Processing {gov_type} in {state}")

                input_df = self.df['s_counties'] if gov_type == "counties" else self.df['s_cities']
                state_places = input_df[input_df['state_code'] == state]

                _output_list = [self._match_urls_to_locations(row, state_places) for row in state_df.itertuples()]
                failed_to_match = sum(1 for result in _output_list if result['href'] is None)

                logger.info(f"Failed to match {failed_to_match} out of {len(state_df)} {gov_type} in {state}")
                output_list.extend(_output_list)

        logger.info(f"Matching took {time.time() - start:.2f} seconds and matched {len(output_list)} places to an href")

        output_df = pd.DataFrame.from_dict(output_list)
        self._save_results(output_df)

        return output_df[output_df['source'].apply(lambda x: self._merge_in_multiple_sources(x))]

    def _merge_in_multiple_sources(self, x):
        if isinstance(x, str) or (isinstance(x, list) and len(x) > 1 and len(set(x)) == len(x)):
            return True
        else:
            return False

    def _save_results(self, output_df: pd.DataFrame) -> None:
        result_dfs = {
            'non_places': self.df['non_places'],
            'matched': output_df[output_df['source'].apply(lambda x: isinstance(x, str))],
            'unmatched': output_df[output_df['source'].isna()],
            'multiple_sources': output_df[output_df['source'].apply(lambda x: isinstance(x, list) and len(x) > 1 and len(set(x)) == len(x))],
            'multiple_matches': output_df[output_df['source'].apply(lambda x: isinstance(x, list) and len(x) > 1 and len(set(x)) < len(x))]
        }

        for name, df in result_dfs.items():
            self._save_to_csv(df, f"{name}.csv")

    @staticmethod
    def _save_to_csv(df: pd.DataFrame, name: str) -> None:
        try:
            if not name.endswith(".csv"):
                raise ValueError("The specified name does not have a '.csv' extension")

            logger.info(f"{len(df)} places were in {name.split('.')[0]}")

            csv_path = os.path.join(OUTPUT_FOLDER, name)
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved {name} to '{csv_path}'")

        except Exception as e:
            logger.debug(f"Could not save {name} to CSV: {e}")




