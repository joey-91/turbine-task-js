import pytest
import pandas as pd
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String
from src.database import create_db, load_to_db

def test_create_db():
    
    # Create a database instance
    engine = create_engine('sqlite:///test_connection')
    
    # Test that the engine can connect to the database
    try:
        with engine.connect() as connection:
            assert connection is not None
    except OperationalError:
        pytest.fail("Database connection failed.")


def test_load_to_db():

    engine = create_db('test_loading') 

    df = pd.DataFrame({
        'column1': [1, 2],
        'column2': ['A', 'B']
    })

    test_schema = Table(
    'test', MetaData(),
    Column('column1', Integer),
    Column('column2', String)
    )


    # Load test df to sql
    load_to_db(df, engine, 'test_table', schema=test_schema)

    # query SQL
    result_df = pd.read_sql('SELECT * FROM test_table', con=engine)

    # Assert that the result matches the original DataFrame
    pd.testing.assert_frame_equal(result_df, df)