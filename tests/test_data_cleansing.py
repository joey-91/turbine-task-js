import pytest
import pandas as pd
from src.data_cleansing import remove_nulls, extract_date

def test_remove_nulls():

    df = pd.DataFrame({
        'column1': [1, 2, None, 4],
        'column2': ['A', None, 'C', 'D']
    })

    result_df = remove_nulls(df)

    expected_df = pd.DataFrame({
        'column1': [1, 4],
        'column2': ['A', 'D']
    })

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_extract_date_valid():
    assert extract_date('2022-03-01 00:00:00') == '2022-03-01'
    assert extract_date('2022-03-02 12:30:00') == '2022-03-02'
    assert extract_date('2021-12-31 23:59:59') == '2021-12-31'