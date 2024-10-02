import pandas as pd

from database import MySqlDatabase

async def get_locations(db: MySqlDatabase) -> pd.DataFrame:
    """
    Fetches location data from the database.

    Args:
        db: MySqlDatabase instance for executing queries.
    Returns:
        A Pandas DataFrame containing location information 'gnis', 'place_name', 'class_code', 'state_code'
    Examples:
    >>> locations_df = get_locations(db)
    >>> locations_df.head()
    # gnis     place_name       class_code  state_code
    2406291    Town of Coker    C1          AL
    1419970    Haines           H1          AK
    2406929    Town of Zinc     C1          AR
    69179      Scott            H1          AR
    2406767    Town of Turin    C1          GA
    """
    command = """
        SELECT gnis, place_name, class_code, state_code FROM locations;
        """
    locations_df: pd.DataFrame = await db.async_query_to_dataframe(command)
    return locations_df
