from typing import Any

import pandas as pd

from database import MySqlDatabase
from config import DATAPOINT
from logger import Logger
logger = Logger(logger_name=__name__)


class InputProcessor:

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

    def validate_datapoint(self) -> bool:
        # Simple validation of the datapoint.
        # We can add more complex crap later.
        return (isinstance(self.datapoint, str) and 
                self.datapoint.strip() != '' and 
                len(self.datapoint) <= 100)

    async def get_initial_dataframe(self) -> pd.DataFrame:
        if not self.validate_datapoint():
            raise ValueError(f"Invalid datapoint: {self.datapoint}")

        safe_format_vars = {
            "datapoint": self.datapoint,
            "limit": f" LIMIT {self.limit};" if self.limit else ";",
            "rand_seed": self.rand_seed or ""
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
            locations_df = await db.query_to_dataframe(query, 
                                                            safe_format_vars=safe_format_vars, 
                                                            unbuffered=self.unbuffered)
            logger.debug(f"locations_df: {locations_df.head()}")
        return locations_df

