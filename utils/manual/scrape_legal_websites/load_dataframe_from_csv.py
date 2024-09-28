
import pandas as pd

async def load_dataframe_from_csv(path: str, header: bool=True) -> pd.DataFrame:
    """
    Load in columns from a csv and put them into a pandas dataframe.

    Parameters:
    -----------
    path : str
        The file path to the CSV file.
    header : bool, optional
        Whether the CSV file has a header row (default is True).

    Returns:
    --------
    pd.DataFrame
        A pandas DataFrame containing the data from the CSV file.
    """
    if header:
        df = pd.read_csv(path)
    else:
        df = pd.read_csv(path, header=None)
    
    # Ensure the DataFrame has at least one column.
    if df.empty or len(df.columns) == 0:
        raise ValueError("The CSV file is empty or has no columns.")

    # Remove any rows with NaN values
    df = df.dropna()

    return df
