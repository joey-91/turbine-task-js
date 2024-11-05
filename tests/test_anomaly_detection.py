import pytest
import pandas as pd
from src.data_transformation import anomaly_detection 

def test_anomaly_detection():
    # Sample data
    data = {
        'turbine_id': [1, 1, 1, 1, 1, 1, 1, 1, 2, 2],
        'date': ['2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01'],
        'power_output': [2, 1, 2, 1, 1, 1, 1, 6, 52, 58]
    }
    df = pd.DataFrame(data)

    # Expected data
    expected_data = {
        'turbine_id': [1, 1, 1, 1, 1, 1, 1, 1, 2, 2],
        'date': ['2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01'],
        'power_output': [2, 1, 2, 1, 1, 1, 1, 6, 52, 58],
        'avg_power_output': [1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 1.88, 55.0, 55.0],
        'stddev_power_output': [1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 1.73, 4.24, 4.24],
        'anomaly': [False, False, False, False, False, False, False, True, False, False]
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the function
    result_df = anomaly_detection(df)

    # Assert that the result DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)