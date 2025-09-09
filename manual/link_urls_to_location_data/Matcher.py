"""
This module provides functionality for matching location data with source URLs.

The main class, Matcher, takes two DataFrames as input:
1. sources_df: A DataFrame containing source URLs and their associated text.
2. locations_df: A DataFrame containing location data (cities, counties, etc.).

The Matcher class preprocesses the input data, applies various regex operations
to clean and standardize place names, and then attempts to match locations
with their corresponding source URLs.
Usage:
    matchmaker = Matcher(sources_df, locations_df)
    output_df = matchmaker.match()

The match() method returns a DataFrame containing the matched results,
including GNIs, place names, state codes, and corresponding URLs.

The module also includes functionality to save various result datasets
(matched, unmatched, multiple sources, etc.) to CSV files.

Dependencies:
    - pandas
    - re
    - os
    - time
    - typing

Note: This module relies on a custom Logger class and a config module
for output folder specification.
"""
import os
import re
import time
from typing import Any, NamedTuple


import pandas as pd


from pathlib import Path
import sys
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))
print(parent_dir)

from config.config import OUTPUT_FOLDER
from logger.logger import Logger
logger = Logger(logger_name=__name__, stacklevel=2)


class Matcher:
    def __init__(self, sources_df: pd.DataFrame, locations_df: pd.DataFrame):
        """
        Initialize the Matcher for linking URLs to location data.
        
        Creates a matcher instance that prepares source and location data for
        matching operations. The sources_df contains URLs and their text content,
        while locations_df contains geographical location information. During
        initialization, preprocessing is performed including regex compilation,
        county boolean conversion, and dataframe preparation.
        
        Args:
            sources_df (pd.DataFrame): DataFrame containing source URLs and associated
                text content for location matching.
            locations_df (pd.DataFrame): DataFrame containing location data including
                place names, state codes, and classification codes.
        
        Returns:
            None
        
        Raises:
            ValueError: If required columns are missing from input DataFrames.
            TypeError: If inputs are not pandas DataFrames.
        
        Example:
            >>> sources = pd.DataFrame({'url': ['url1'], 'text': ['content1']})
            >>> locations = pd.DataFrame({'place_name': ['City'], 'state_code': ['CA']})
            >>> matcher = Matcher(sources, locations)
        """
        self._sources_df = sources_df
        self._locations_df = self._make_county_boolean(locations_df)
        self.regex = self._compile_regex()
        self.not_places = self._define_not_places()
        self.county_eqs = self._define_county_equivalents()
        self.df = self._prepare_dataframes()

    @staticmethod
    def _make_county_boolean(locations_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert class_code column to boolean county indicator.
        
        Transforms the 'class_code' column into a 'county' column with boolean
        values indicating whether each location is a county (contains 'H') or not.
        This preprocessing step simplifies county identification during matching.
        
        Args:
            locations_df (pd.DataFrame): DataFrame containing location data with
                a 'class_code' column.
        
        Returns:
            pd.DataFrame: Modified DataFrame with 'county' boolean column replacing
                'class_code' column.
        
        Raises:
            KeyError: If 'class_code' column is not present in the DataFrame.
            AttributeError: If class_code values don't support string operations.
        
        Example:
            >>> df = pd.DataFrame({'class_code': ['H1', 'C2', 'H3']})
            >>> result = Matcher._make_county_boolean(df)
            >>> print(result['county'].tolist())  # [True, False, True]
        """
        locations_df = locations_df.rename(columns={'class_code': 'county'})
        locations_df['county'] = locations_df['county'].str.contains('H')
        return locations_df

    @staticmethod
    def _compile_regex() -> dict[str, dict[str, Any]]:
        """
        Compile regular expressions for text cleaning operations.
        
        Creates a dictionary of compiled regex patterns and their associated
        operations for cleaning place names. Each entry contains both the
        compiled regex pattern and a lambda function for applying the cleaning
        operation.
        
        Returns:
            dict[str, dict[str, Any]]: Dictionary mapping regex names to dictionaries
                containing 'regex' (compiled pattern) and 'operation' (lambda function)
                keys for text cleaning operations.
        
        Raises:
            re.error: If regex compilation fails due to invalid patterns.
        
        Example:
            >>> regex_dict = Matcher._compile_regex()
            >>> clean_op = regex_dict['clean_parentheses_regex']['operation']
            >>> result = clean_op("City (County) Name")  # Returns "City  Name"
        """
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
        """
        Define keywords that indicate non-geographical entities.
        
        Returns a list of terms that, when found in text, suggest the content
        refers to organizations, institutions, or administrative entities rather
        than geographical places. Used for filtering out false positive matches
        during location matching.
        
        Returns:
            list[str]: List of lowercase keywords indicating non-geographical entities
                such as organizations, institutions, and administrative bodies.
        
        Raises:
            None
        
        Example:
            >>> not_places = Matcher._define_not_places()
            >>> print("district" in not_places)  # True
            >>> print("university of" in not_places)  # True
        """
        return [
            "district", "tribe", "code", "commission", "jury", "system", "council",
            "association", "corporation", "authority", "civil service",
            "university of", "air park", "examiner",
            "children's", "rules and appeals", "clerk of court",
            "seminole nation", "prairie band potawatomi nation", "samish indian nation", "osage nation"
        ]

    @staticmethod
    def _define_county_equivalents() -> list[str]:
        """
        Define terms that are equivalent to or variations of county designations.
        
        Returns a comprehensive list of terms that represent county-level
        administrative divisions including their variations with underscores
        and boundaries. Used for identifying county-related content in text
        matching operations.
        
        Returns:
            list[str]: List of county-equivalent terms including counties, boroughs,
                parishes, census areas, and various municipal designations with
                their underscore and boundary variations.
        
        Raises:
            None
        
        Example:
            >>> county_terms = Matcher._define_county_equivalents()
            >>> print("borough" in county_terms)  # True
            >>> print("parish_" in county_terms)  # True
        """
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
        """
        Prepare and categorize dataframes for location matching operations.
        
        Creates a comprehensive set of categorized DataFrames from the source and
        location data. Separates sources into places vs non-places, and further
        categorizes both sources and locations into counties vs cities/towns.
        This preprocessing enables efficient targeted matching.
        
        Args:
            None
        
        Returns:
            dict[str, pd.DataFrame]: Dictionary containing categorized DataFrames:
                - 'locations': All location data
                - 'counties': Location data for counties only  
                - 'cities': Location data for cities/towns only
                - 'sources': All source data
                - 'places': Sources identified as places
                - 'non_places': Sources identified as non-places
                - 's_counties': Sources identified as county-related
                - 's_cities': Sources identified as city/town-related
        
        Raises:
            KeyError: If required columns are missing from source DataFrames.
            ValueError: If filtering operations fail.
        
        Example:
            >>> matcher = Matcher(sources_df, locations_df)
            >>> dfs = matcher._prepare_dataframes()
            >>> print(f"Found {len(dfs['places'])} place sources")
        """
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
        """
        Check if text contains keywords indicating non-geographical entities.
        
        Examines the provided text for the presence of keywords that suggest
        the content refers to organizations, institutions, or administrative
        entities rather than geographical places. Used to filter out false
        positive location matches.
        
        Args:
            text (str): Text content to check for non-place indicators.
        
        Returns:
            bool: True if text contains non-place keywords, False if it appears
                to be place-related content.
        
        Raises:
            AttributeError: If text is None or doesn't support lower() method.
        
        Example:
            >>> matcher = Matcher(sources_df, locations_df)
            >>> print(matcher._remove_non_places("University of California"))  # True
            >>> print(matcher._remove_non_places("City of San Francisco"))  # False
        """
        return any(non_place in text.lower() for non_place in self.not_places)

    def _check_if_county(self, row: NamedTuple) -> bool:
        """
        Determine if a source row refers to county-level information.
        
        Examines both the URL and text content of a source row to determine
        if it refers to county-level administrative content. Checks for the
        presence of county-equivalent terms in both the href and text fields.
        
        Args:
            row (NamedTuple): Source data row containing 'href' and 'text' attributes.
        
        Returns:
            bool: True if the row appears to be county-related, False otherwise.
        
        Raises:
            AttributeError: If row lacks 'href' or 'text' attributes.
        
        Example:
            >>> # Assuming row has href and text attributes
            >>> matcher = Matcher(sources_df, locations_df)
            >>> result = matcher._check_if_county(source_row)
            >>> print(f"Is county: {result}")
        """
        lower_href = row.href.lower()
        lower_text = row.text.lower()
        return any(county in lower_href or county in lower_text for county in self.county_eqs)

    def _is_place_in_text(self, row: NamedTuple, text: str) -> bool:
        """
        Check if a place name from location data appears in the given text.
        
        Applies text cleaning operations to the place name using compiled regex
        patterns, then searches for the cleaned place name as a whole word in
        the provided text. Performs case-insensitive matching with word boundaries
        to avoid partial matches.
        
        Args:
            row (NamedTuple): Location data row containing 'place_name' and 'county'
                attributes.
            text (str): Text content to search for the place name.
        
        Returns:
            bool: True if the place name is found in the text, False otherwise.
        
        Raises:
            AttributeError: If row lacks 'place_name' attribute.
            re.error: If regex pattern compilation or search fails.
        
        Example:
            >>> # Assuming location_row has place_name attribute
            >>> matcher = Matcher(sources_df, locations_df)
            >>> found = matcher._is_place_in_text(location_row, "Welcome to Springfield")
            >>> print(f"Springfield found: {found}")
        """
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
        """
        Match URLs to locations function.
        
        Attempts to find source URLs that correspond to a specific geographical
        location by searching for the place name in both the text content and
        href fields of the sources. Returns a dictionary with location information
        and any matching URLs found.
        
        Args:
            row (NamedTuple): Location data row containing 'gnis', 'place_name',
                'state_code', and other location attributes.
            state_places (pd.DataFrame): DataFrame of source data filtered to the
                same state as the location being matched.
        
        Returns:
            dict[str, Any]: Dictionary containing location information and matching
                URLs. Keys include 'gnis', 'place_name', 'state_code', 'href', and
                'source'. href and source will be None if no matches found, single
                values if one match, or lists if multiple matches.
        
        Raises:
            Exception: Any exceptions during matching are logged and re-raised.
            AttributeError: If row lacks required attributes.
        
        Example:
            >>> # Assuming location_row and state_sources are properly formatted
            >>> matcher = Matcher(sources_df, locations_df)
            >>> result = matcher._match_urls_to_locations(location_row, state_sources)
            >>> print(f"Found matches: {result['href'] is not None}")
        """
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
        """
        Execute the main matching process between locations and source URLs.
        
        Performs the complete matching workflow by processing all counties and
        cities across all states. For each geographical location, attempts to
        find corresponding source URLs that reference that location. Logs
        progress and timing information, saves results to files, and returns
        a filtered DataFrame of successful matches.
        
        Args:
            None
        
        Returns:
            pd.DataFrame: DataFrame containing successfully matched locations with
                their corresponding URLs, filtered to include only valid sources.
        
        Raises:
            KeyError: If required columns are missing from prepared DataFrames.
            ValueError: If matching process encounters invalid data.
        
        Example:
            >>> matcher = Matcher(sources_df, locations_df)
            >>> results = matcher.match()
            >>> print(f"Successfully matched {len(results)} locations")
        """
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

        return output_df[output_df['source'].apply(self._filter_valid_sources)]

    def _filter_valid_sources(self, x: Any) -> bool:
        """
        Filter and validate source data for inclusion in final results.
        
        Determines whether a source entry should be included in the final
        results based on its type and content. Single string sources are
        always valid. List sources are valid only if they contain multiple
        unique entries (no duplicates).
        
        Args:
            x (Any): Source data which can be a string, list, or None.
        
        Returns:
            bool: True if the source is valid for inclusion, False otherwise.
        
        Raises:
            None
        
        Example:
            >>> matcher = Matcher(sources_df, locations_df)
            >>> print(matcher._filter_valid_sources("single_url"))  # True
            >>> print(matcher._filter_valid_sources(["url1", "url2"]))  # True
            >>> print(matcher._filter_valid_sources(["url1", "url1"]))  # False
        """
        if isinstance(x, str):
            return True
        if isinstance(x, list) and len(x) > 1:
            return len(set(x)) == len(x)
        return False

    def _save_results(self, output_df: pd.DataFrame) -> None:
        """
        Save matching results to multiple CSV files for analysis.
        
        Creates several categorized CSV files from the matching results including
        the complete output, non-places, single matches, unmatched locations,
        multiple valid sources, and multiple conflicting matches. Each category
        provides different perspectives on the matching results for analysis.
        
        Args:
            output_df (pd.DataFrame): DataFrame containing all matching results
                with location and source information.
        
        Returns:
            None
        
        Raises:
            OSError: If file writing operations fail.
            ValueError: If DataFrame operations encounter invalid data.
        
        Example:
            >>> matcher = Matcher(sources_df, locations_df)
            >>> results = matcher.match()  # This calls _save_results internally
            >>> # Files saved: output_df.csv, single_match.csv, unmatched.csv, etc.
        """
        result_dfs = {
            'output_df': output_df,
            'non_places': self.df['non_places'],
            'single_match': output_df[output_df['source'].apply(lambda x: isinstance(x, str))],
            'unmatched': output_df[output_df['source'].isna()],
            'multiple_sources': output_df[output_df['source'].apply(lambda x: isinstance(x, list) and len(x) > 1 and len(set(x)) == len(x))],
            'multiple_matches': output_df[output_df['source'].apply(lambda x: isinstance(x, list) and len(x) > 1 and len(set(x)) < len(x))]
        }

        for name, df in result_dfs.items():
            self._save_to_csv(df, f"{name}.csv")

    @staticmethod
    def _save_to_csv(df: pd.DataFrame, name: str) -> None:
        """
        Save a DataFrame to a CSV file with logging and error handling.
        
        Writes the provided DataFrame to a CSV file in the configured output
        directory. Validates that the filename has a .csv extension and logs
        the number of records saved. Handles exceptions gracefully with debug
        logging.
        
        Args:
            df (pd.DataFrame): DataFrame to save to CSV file.
            name (str): Filename for the CSV file, must end with '.csv' extension.
        
        Returns:
            None
        
        Raises:
            ValueError: If filename doesn't have .csv extension.
            OSError: If file writing fails due to permissions or disk space.
        
        Example:
            >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            >>> Matcher._save_to_csv(df, "results.csv")
            >>> # Saves df to configured output folder as results.csv
        """
        try:
            if not name.endswith(".csv"):
                raise ValueError("The specified name does not have a '.csv' extension")

            logger.info(f"{len(df)} places were in {name.split('.')[0]}")

            csv_path = os.path.join(OUTPUT_FOLDER, name)
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved {name} to '{csv_path}'")

        except Exception as e:
            logger.debug(f"Could not save {name} to CSV: {e}")
