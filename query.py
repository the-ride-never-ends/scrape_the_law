
from typing import Any
import re

import pandas as pd

from utils.query.extract_and_process_place_name import extract_and_process_place_name
from utils.shared.make_sha256_hash import make_sha256_hash

from config import MUNICODE_URL, AMERICAN_LEGAL_URL, GENERAL_CODE_URL, CODE_PUBLISHING_CO_URL, SEARCH_ENGINE
from database import MySqlDatabase
from logger import Logger
log_level = 20
logger = Logger(logger_name=__name__,log_level=log_level)



class SearchQueryGenerator:

    def __init__(self, 
                 datapoint,
                 common_terms:list=None,
                 search_engine: str="google"
                ):
        self.datapoint = datapoint
        self.common_terms = common_terms
        self.search_engine = search_engine

        if not self.common_terms:
            raise ValueError("SearchQueryGenerator missing common_terms input")
        
        if search_engine != "google":
            raise NotImplementedError("SearchQueryGenerator cannot make queries for non-Google search engines at this time.")


    def make_municode_query(self, row):
        """
        Constructs a search query for the Municode website.

        Example:
            >>> row = NamedTuple(state_code="CA", place_name="Los Angeles")
            >>> make_municode_query(row, "zoning ordinance")
            ('site:https://library.municode.com/ca/los_angeles "zoning ordinance"', 'municode')
        """
        source = "municode"

        # Convert the state code to lowercase
        state_code_lower = row.state_code.lower()

        # "City of Los Angeles" -> los_angeles
        formatted_place = row.place_name.split(' of ')[-1].lower().replace(' ', '_')

        # Construct the base query using the site: operator and the Municode URL
        # NOTE: A space is left at the end for concatenation
        municode_url = f"site:{MUNICODE_URL}{state_code_lower}/{formatted_place}/"

        # Construct the full query by combining:
        # 1. The base Municode query
        # 2. The lowercased datapoint in quotes for exact matching
        query = municode_url + f' "{self.datapoint.lower()}"'

        return query, source


    def make_american_legal_query(self, row):
        """
        Constructs a search query for the American Legal Publishing website.

        Example:
            >>> row = NamedTuple(state_code="CA", place_name="Los Angeles")
            >>> make_american_legal_query(row, "zoning ordinance")
            ('site:https://codelibrary.amlegal.com/codes/losangelesca/latest/losangeles_ca/ "zoning ordinance"', 'american_legal')
        """
        source = "american_legal"

        # Convert the state code to lowercase
        state_code_lower = row.state_code.lower()

        # Process and lowercase the place name
        formatted_place = extract_and_process_place_name(row.place_name)

        # Concatenate place name and state code
        place_state_concat = formatted_place + state_code_lower
        place_state_concat_with_underscore = formatted_place + "_" + state_code_lower

        # Construct the base query using the American Legal URL and processed place/state information
        american_legal_query = f"site:{AMERICAN_LEGAL_URL}{place_state_concat}/latest/{place_state_concat_with_underscore}/ "

        # Append the lowercased datapoint to the query, wrapped in quotes
        query = american_legal_query + f'"{self.datapoint.lower()}"'

        # Return the query and source as a tuple
        return query, source


    def make_general_code_query(self, row):
        """
        Constructs a search query for the General Code website.

        Example:
            >>> row = NamedTuple(place_name="Springfield", state_code="IL")
            >>> make_general_code_query(row, "zoning ordinance")
            ('https://www.generalcode.com/library/ Springfield IL "zoning ordinance"', 'general_code')
        """
        # Set the source identifier
        source = "general_code"

        # Construct the query string
        # Combine the base URL, place name, state code, and the lowercased self.datapoint in quotes
        query = f'site:{GENERAL_CODE_URL} "{row.place_name}" {row.state_code} "{self.datapoint.lower()}"'

        # Return the query and source as a tuple
        return query, source


    def make_code_publishing_co_query(self, row):
        """
        Constructs a search query for the Code Publishing Company website.

        Example:
            >>> row = NamedTuple(place_name="Springfield", state_code="IL")
            >>> make_code_publishing_co_query(row, "zoning ordinance")
            ('site:https://www.codepublishing.com/ Springfield IL "zoning ordinance"', 'code_publishing_co')
        """
        # Set the source identifier
        source = "code_publishing_co"

        # Construct the query string
        # Use the site: operator to limit the search to the Code Publishing Co. website
        # Include the place name and state code from the row data
        # Add the lowercased self.datapoint in quotes for exact matching
        query = f'site:{CODE_PUBLISHING_CO_URL} {row.place_name} {row.state_code} "{self.datapoint.lower()}"'

        # Return the query and source as a tuple
        return query, source


    def make_domain_name_query(self, row):
        """
        Constructs a search query for a place's domain name.
        
        Example:
            >>> row = NamedTuple(domain_name="example.gov")
            >>> _construct_domain_name_query(row, "zoning ordinance")
            ('site:example.gov "zoning ordinance"', 'place_domain')
        """
        # Set the source identifier
        source = "place_domain"
        
        # Extract the domain name from the row data
        domain_name = row.domain_name
        
        # Construct the base query using the site: operator
        domain_query = f"site:{domain_name} "

        # Append the lowercased self.datapoint to the query, wrapped in quotes
        query = domain_query + f'"{self.datapoint.lower()}"'

        # Return the query and source as a tuple
        return query, source


    async def make_queries(self, df: pd.DataFrame, source=None, set_queries: set=None) -> pd.DataFrame:
        """
        Construct unique search engine queries to find document URLs for given places in the USA.
        Supports different query construction methods for various websites, location types (places vs counties) and
        can filter queries based on a specified source.
        - NOTE: These query strings are currently tuned for Google. Bing and others will have to be done separately.

        Args:
            places_df: DataFrame containing place information.
            datapoint: The type of data being queried. Defaults to DATAPOINT constant.
            source: The specific source to filter queries for. Defaults to "municode".
            search_engine: The search engine to construct queries for. Defaults to "google".
            set_queries: A set of pre-existing query tuples to avoid duplicates. Defaults to None.

        Returns:
            A DataFrame containing the constructed queries with columns "gnis", "queries", and "source".

        Example:
            >>> from database import MySqlDatabase
            >>> import pandas as pd
            >>> async with MySqlDatabase(database="socialtoolkit") as db:
            >>>     # Get a list of places from the sql database where we got domain URLs
            >>>     places = await db.async_execute_sql_command(
            >>>     /"/"/"
            >>>     SELECT gnis, place_name, state_code, class_code, domain_name FROM locations 
            >>>     WHERE domain_name IS NOT NULL;
            >>>     /"/"/"
            >>>     )
            >>>     var_types = {
            >>>         'gnis': 'int32',
            >>>         'place_name': 'string',
            >>>         'state_code': 'string',
            >>>         'class_code': 'string',
            >>>         'domain_name': 'string'
            >>>     } 
            >>>     places_df = pd.DataFrame(
            >>>            places, 
            >>>            columns=["gnis", "place_name", "state_code", "class_code", "domain_name"]
            >>>            ).astype(var_types)
            >>>     # Generate queries based on the location.
            >>>     queries_df = make_queries(places_df, DATAPOINT)
            >>> return queries_df
        """

        # Load in a set of already-created query tuples, if applicable.
        if not set_queries:
            set_queries = set()
        else:
            assert len(set_queries[0]) == 3, f"tuples in set_queries are len {len(set_queries[0])}, not 3"

        # Initialize data as a set.
        # This will prevent any duplicate queries that might be generated.
        data = set()

        # Create a list of query construction functions.
        # Each function spits out a unique query specifically tuned for that website.
        # NOTE We want the query url to be as specific as possible to reduce strain on the websites and avoid detection.
        function_list = [
            self.make_municode_query,
            self.make_american_legal_query,
            self.make_general_code_query,
            self.make_code_publishing_co_query,
            self.make_domain_name_query
        ]

        logger.info(f"Starting construct_queries for-loop. Input URL count: {len(df)}")
        for i, row in enumerate(df.itertuples(), start=1):
            logger.debug(f"Processing row {i} of {len(df)}...")

            skip_to_next_row = False
            for func in function_list:
                # Counties and Places get separate routes.
                # TODO Create county paths for these functions
                county = False if "H" not in row.class_code else True

                try: # Make the query strings
                    query, tuple_source = func(row)
                except Exception as e:
                    logger.warning(f"Failed to construct query_tuple with '{func}': {e}. Skipping...")
                    skip_to_next_row = True

                if source: # Skip the query if it's not for the source we want, if applicable.
                    if tuple_source != source:
                        logger.debug(f"tuple_source '{tuple_source}' does not match the desired source '{source}'. Skipping...")
                        skip_to_next_row = True

                # Create a query_tuple if it's not in set_queries.
                if query is not any(query[1] == query_tuple[1] for query_tuple in set_queries):
                    query_hash = make_sha256_hash(row.gnis, query, SEARCH_ENGINE)
                    query_tuple = (row.gnis, query, tuple_source, query_hash)
                    data.add(query_tuple)
                    logger.debug("Query tuple created and added to data list.")
                else:
                    logger.debug("Query tuple already exists in set_queries. Skipping...")
                    skip_to_next_row = True

            if skip_to_next_row:
                continue

        # Reformat the data so that it's a list.
        data = list(data)
        len_query = len(data[0][1])
        assert len_query == 5 or 1, f"query in query_tuple is length '{len_query}', not 5 or 1"
        logger.info(f"construct_queries function created {len(data)} unique queries. Returning data list as DataFrame...")

        return pd.DataFrame(data, columns=["gnis", "query", "source", "query_hash"])




# if __name__ == "__main__":
#     # Example usage
#     generator = SearchQueryGenerator()
    
    
#     #queries = generator.generate_queries(datapoint, location)
    
#     print(f"Generated {len(queries)} queries:")
#     for query in queries:
#         print(query)

        # return pd.DataFrame(queries)

        # # Clean and prepare the datapoint
        # clean_datapoint = re.sub(r'[^\w\s]', '', datapoint).lower()
        # logger.debug(f"clean_datapoint: {clean_datapoint}")

        # # Extract location information
        # place_name = location.get('place_name', '').strip()
        # state_name = location.get('state_name', '').strip()
        # domain_name = location.get('domain_name', '').strip()

        # # Generate queries
        # queries.append(f'site:{domain_name} "{clean_datapoint}"')
        # queries.append(f'"{place_name}" "{state_name}" "{clean_datapoint}"')

        # for term in self.common_terms:
        #     queries.append(f'"{place_name}" "{state_name}" "{clean_datapoint}" {term}')
        #     if domain_name:
        #         queries.append(f'site:{domain_name} "{clean_datapoint}" {term}')

        # # Add some variations
        # queries.append(f'"{place_name}" "{clean_datapoint}" law')
        # queries.append(f'"{state_name}" local "{clean_datapoint}" regulation')

        # # Remove any duplicate queries
        # return list(set(queries))
