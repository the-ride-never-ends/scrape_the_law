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

        return output_df[output_df['source'].apply(self._filter_valid_sources)]

    def _filter_valid_sources(self, x: Any) -> bool:
        if isinstance(x, str):
            return True
        if isinstance(x, list) and len(x) > 1:
            return len(set(x)) == len(x)
        return False

    def _save_results(self, output_df: pd.DataFrame) -> None:
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
        try:
            if not name.endswith(".csv"):
                raise ValueError("The specified name does not have a '.csv' extension")

            logger.info(f"{len(df)} places were in {name.split('.')[0]}")

            csv_path = os.path.join(OUTPUT_FOLDER, name)
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved {name} to '{csv_path}'")

        except Exception as e:
            logger.debug(f"Could not save {name} to CSV: {e}")
