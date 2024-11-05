import pytest
import pandas as pd
from src.data_ingestion import concat_files

def test_concat_files(tmp_path):
    # Create sample CSVs in a temporary directory
    df1 = pd.DataFrame({'col_1': [1, 2], 'col_2': ['A', 'B']})
    df2 = pd.DataFrame({'col_1': [3, 4], 'col_2': ['C', 'D']})
    df1.to_csv(tmp_path / 'file1.csv', index=False)
    df2.to_csv(tmp_path / 'file2.csv', index=False)

    # Call concat function with temporary directory
    result_df = concat_files(filepath=tmp_path)

    # Expected DataFrame after concatenation
    expected_df = pd.concat([df1, df2], ignore_index=True)

    # Assert that the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)