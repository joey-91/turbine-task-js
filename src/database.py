from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData, Table
import pandas as pd

def create_db(db_name: str):
    """
    Creates a connection to the SQLite database specified by db_path.
    
    Parameters:
        db_name (str): Name of database.
        
    Returns:
        engine: A SQLAlchemy engine connected to the database.
    """
    engine = create_engine(f'sqlite:///{db_name}')
    return engine

def load_to_db(df: pd.DataFrame, engine: Engine, table_name: str, schema: Table):
    """
    Loads dataframe to sql database

    Parameters:
        df (pd.DataFrame:): dataframe object
        db_name (sqlite object): database name
        table_name: desired table name
        
    """

    schema.metadata.create_all(engine)

    return df.to_sql(table_name, con=engine, if_exists='replace', index=False)

