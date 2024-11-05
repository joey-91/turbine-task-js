import pandas as pd

def remove_nulls(df: pd.DataFrame):
    """
    Removes all rows with null values from df.

    Parameters:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: A DataFrame with all null values removed.
    """
    return df.dropna()

def extract_date(timestamp_str: str):
    """
    Extracts the date from a timestamp string.

    Parameters:
        timestamp_str (str): The timestamp string.

    Returns:
        str: The extracted date as a string in 'YYYY-MM-DD' format.
    """
    return pd.to_datetime(timestamp_str).strftime('%Y-%m-%d')
