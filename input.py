from typing import Any

import pandas as pd

from database import MySqlDatabase
from config import DATAPOINT
from logger import Logger
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
        Validate the datapoint attribute.\n
        TODO Simple validation at the moment, but will likely need to be expanded in the future.\n
        Including aliases for datapoints is a must eventually.
        """
        return (isinstance(self.datapoint, str) and
                self.datapoint.strip() != '' and
                len(self.datapoint) <= 100)

    async def get_initial_dataframe(self) -> pd.DataFrame:
        """
        Fetch and return a DataFrame of location data based on the datapoint.
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
