import os
import pandas as pd

def concat_files(filepath: str):
    """
    Converts all CSV files in filepath to a single dataframe
        
    Returns:
        pd.DataFrame: a single df containing all data in filepath
    """

    dfs = []
    
    # Iterate over all files in the directory
    for filename in os.listdir(filepath):
        if filename.endswith('.csv'):
            file_path = os.path.join(filepath, filename)
            df = pd.read_csv(file_path)
            dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)


