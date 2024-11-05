import os
import re


import pandas as pd
from datetime import datetime

from config.config import OUTPUT_FOLDER
from logger.logger import Logger

logger = Logger(logger_name=__name__)



def pivot_df_from_long_to_wide(output_df: pd.DataFrame) -> pd.DataFrame:
    """
    gnis, state_code, href, source

    Input:
              gnis          place_name state_code                                        href        source
    0      2419458  City of Saint George         AK                                         NaN           NaN
    1      2419441    City of Nunam Iqua         AK                                         NaN           NaN
    2      2419440        City of Nulato         AK                                         NaN           NaN
    3      2419368       City of Chignik         AK                                         NaN           NaN
    4      2418862    City of Thorne Bay         AK                                         NaN           NaN
    ...        ...                   ...       ...                                         ...           ...
    23083  2412710      Town of Greybull         WY    https://library.municode.com/wy/greybull      municode
    23084  2412088    City of Torrington         WY  https://library.municode.com/wy/torrington      municode
    23085  2413134      Town of Pinedale         WY                 https://ecode360.com/PI2813  general_code
    23086  1605074           Hot Springs         WY                                         NaN           NaN
    23087  2410350       City of Douglas         WY     https://library.municode.com/wy/douglas      municode

    Output:
           id     gnis            place_name class_code state_code source_municode source_general_code source_american_legal source_code_publishing_co
    0       1  2406291        Town of Coker         C1         AL             NaN                 NaN                   NaN                       NaN
    1       2  1419970               Haines         H1         AK             NaN                 NaN                   NaN                       NaN
    2       3  2406929         Town of Zinc         C1         AR             NaN                 NaN                   NaN                       NaN
    3       4    69179                Scott         H1         AR             NaN                 NaN                   NaN                       NaN
    """

    # Create a copy of the input dataframe to avoid modifying the original
    df = output_df.copy()
    
    # Ensure 'gnis' and 'state_code' are present in the dataframe
    if 'gnis' not in df.columns or 'state_code' not in df.columns:
        raise ValueError("Input dataframe must contain 'gnis' and 'state_code' columns")
    logger.debug(f"Verified 'gnis' and 'state_code' columns.\nColumns\n{df.columns.tolist()}",f=True)

    # Create a dictionary to map source names to column names
    source_map = {
        'municode': 'source_municode',
        'general_code': 'source_general_code',
        'american_legal': 'source_american_legal',
        'code_publishing_co': 'source_code_publishing_co',
    }
    logger.debug(f"Created source_map: {source_map}")

    # Initialize new columns with NaN values
    for col in source_map.values():
        df[col] = pd.NA
    logger.debug(f"Initialized new columns. New columns: {source_map.values()}",f=True)

    # Reformat american legal hrefs so that they link correctly.
    amlegal_pattern = r"regions/[a-z]{2}/"
    def clean_amlegal_href(href):
        if isinstance(href, str) and "amlegal" in href:
            return re.sub(amlegal_pattern, '', href)
        return href
    df['href'] = df['href'].apply(clean_amlegal_href)

    # Iterate through rows and populate the new columns
    for idx, row in df.iterrows():
        if pd.notna(row['source']) and pd.notna(row['href']):
            source_col = source_map.get(row['source'])
            if source_col:
                df.at[idx, source_col] = row['href']
    logger.debug(f"Populated new columns\nSample row\n{df.iloc[0].to_dict()}",f=True)
    
    # Drop the original 'href' and 'source' columns
    df = df.drop(['href', 'source'], axis=1)
    logger.debug(f"Dropped 'href' and 'source' columns.\nRemaining columns\n{df.columns.tolist()}",f=True)
    
    # Reorder columns to match the desired output
    column_order = ['gnis', 'place_name', 'state_code'] + list(source_map.values())
    df = df[column_order]
    logger.debug(f"Reordered columns.\nNew order\n{df.columns.tolist()}")

    # Drop columns with no sources.
    source_columns = ["source_municode", "source_general_code", "source_american_legal", "source_code_publishing_co"]
    df = drop_rows_missing_all_sources(df, source_columns)

    # df = df.dropna(subset=source_columns)
    path = os.path.join(OUTPUT_FOLDER,"final_sql_insert.csv")
    df.to_csv(path)

    # Display the result
    logger.debug(f"Final dataframe shape: {df.shape}\nFinal dataframe head\n{df.head()}",f=True)
    logger.debug(f"\nFinal dataframe\n{df}",f=True)
    return df


def drop_rows_missing_all_sources(df: pd.DataFrame, source_columns: list[str]) -> pd.DataFrame:
    """
    Drop rows from a DataFrame if they are missing all the data in specified source columns.

    Args:
        df (pd.DataFrame): Input DataFrame
        source_columns (list[str]): List of columns in the dataframe

    Returns:
        pd.DataFrame: DataFrame with rows removed where all specified source columns are null or '<NA>'
    """
    # Check if all specified columns exist in the DataFrame
    missing_columns = [col for col in source_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"The following columns are missing from the DataFrame: {missing_columns}")

    # Create a boolean mask where True indicates all specified columns are null or '<NA>'
    mask = df[source_columns].apply(lambda x: x.isna() | (x == '<NA>')).all(axis=1)

    # Drop rows where all specified columns are null or '<NA>' and reset the index
    df_cleaned = df[~mask].reset_index(drop=True)

    # Log the number of rows dropped
    rows_dropped = len(df) - len(df_cleaned)
    print(f"Dropped {rows_dropped} rows where all specified source columns were null or '<NA>'.")

    return df_cleaned

