import pytest
import pandas as pd
from sqlalchemy import create_engine
from src.data_transformation import statistics_table  

@pytest.fixture
def sample_data():
    # Sample data similar to what would be in 'silver_turbines'
    data = {
        'turbine_id': [1, 1, 2],
        'date': ['2022-03-01', '2022-03-01', '2022-03-02'],
        'power_output': [50, 55, 60]
    }
    return pd.DataFrame(data)

@pytest.fixture
def setup_db(sample_data):
    engine = create_engine('sqlite:///test_statistics')
    sample_data.to_sql('silver_turbine_test', con=engine, index=False, if_exists='replace')
    return engine

def test_statistics_table(setup_db):
    engine = setup_db
    result_df = statistics_table(engine=engine, table_name='silver_turbine_test') 
    
    # Expected data for verification
    expected_data = {
        'turbine_id': [1, 2],
        'date': ['2022-03-01', '2022-03-02'],
        'avg_power_output': [52.50, 60.00],
        'min_power_output': [50.00, 60.00],
        'max_power_output': [55.00, 60.00]
    }
    expected_df = pd.DataFrame(expected_data)
    
    # Assert that the result DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)
