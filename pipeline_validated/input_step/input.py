from typing import Any

import pandas as pd

from database.database import MySqlDatabase
from config.config import DATAPOINT
from logger.logger import Logger
logger = Logger(logger_name=__name__)


class InputProcessor:
    """
    Processes input data for location-based operations.
    
    This class handles the validation of input datapoints and retrieves
    location data from a MySQL database based on specified criteria.

    Parameters
    -----------
    datapoint : Any, default DATAPOINT
        The input data point to process.
    rand_seed : int, optional
        Seed for controlled yet random ordering of MySQL query results.
    limit : int, optional
        Maximum number of results to retrieve.
    unbuffered : bool, default False
        Whether to use unbuffered database queries.
    """

    def __init__(self,
                 datapoint: Any = DATAPOINT,
                 rand_seed: int = None,
                 limit: int = None,
                 unbuffered: bool = False
                 ):
        self.datapoint = datapoint
        self.rand_seed = rand_seed
        self.limit = limit
        self.unbuffered = unbuffered

    def _validate_datapoint(self) -> bool:
        """
        Validate the datapoint attribute for basic requirements.
        
        Performs simple validation to ensure the datapoint is a non-empty string
        within reasonable length limits. This validation may be expanded in future
        versions to include aliases and more sophisticated checks.
        
        Args:
            None
        
        Returns:
            bool: True if datapoint is valid, False otherwise.
        
        Raises:
            None
        
        Example:
            >>> processor = InputProcessor(datapoint="sales tax")
            >>> processor._validate_datapoint()
            True
            >>> processor.datapoint = ""
            >>> processor._validate_datapoint()
            False
        """
        return (isinstance(self.datapoint, str) and
                self.datapoint.strip() != '' and
                len(self.datapoint) <= 100)

    async def get_initial_dataframe(self) -> pd.DataFrame:
        """
        Fetch and return a DataFrame of location data based on the datapoint.
        
        Queries the database for location records that have domain names but haven't
        been searched for the current datapoint yet. Returns a randomized selection
        to ensure varied processing across different runs.
        
        Args:
            None
        
        Returns:
            pd.DataFrame: DataFrame containing columns: id, gnis, place_name, 
                class_code, state_code, domain_name for qualifying locations.
        
        Raises:
            ValueError: If the datapoint fails validation.
            Exception: For database connection or query errors.
        
        Example:
            >>> processor = InputProcessor(datapoint="sales tax", limit=5)
            >>> df = await processor.get_initial_dataframe()
            >>> list(df.columns)
            ['id', 'gnis', 'place_name', 'class_code', 'state_code', 'domain_name']
            >>> len(df) <= 5
            True
        """
        if not self._validate_datapoint():
            raise ValueError(f"Invalid datapoint: {self.datapoint}")

        args = {
            "datapoint": self.datapoint,
            "rand_seed": self.rand_seed or "",
            "limit": f" LIMIT {self.limit};" if self.limit else ";"
        }

        query = """
        SELECT DISTINCT l.id, l.gnis, l.place_name, l.class_code, l.state_code, l.domain_name
        FROM locations l
        LEFT JOIN searches s ON l.gnis = s.gnis
        WHERE l.domain_name IS NOT NULL AND
        s.gnis IS NULL OR
        s.query_text NOT LIKE '%{datapoint}%'
        ORDER BY RAND({rand_seed}){limit}
        """

        async with MySqlDatabase(database="socialtoolkit") as db:
            locations_df = await db.async_query_to_dataframe(query,
                                                            args=args,
                                                            unbuffered=self.unbuffered)
            logger.debug(f"locations_df: {locations_df.head()}")
        return locations_df
