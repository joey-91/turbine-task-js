import sqlalchemy
import pandas as pd
from src.database import create_db, load_to_db
from src.data_ingestion import concat_files
from src.data_cleansing import remove_nulls, extract_date
from src.data_transformation import anomaly_detection, statistics_table
from src.schemas import bronze_schema, silver_schema, gold_schema, gold_statistics_schema

ENGINE = create_db(db_name='turbine_data.db')

def bronze(engine):
    raw_df = concat_files(filepath='./data')
    print(raw_df.head())

    # Create Bronze table
    load_to_db(df=raw_df, engine=engine, table_name='bronze_turbines', schema=bronze_schema)
    return

def silver(engine):
    bronze_df = pd.read_sql('SELECT * FROM bronze_turbines', con=engine)

    # Create Cleansed table
    bronze_df_rn = remove_nulls(bronze_df)
    bronze_df_rn['date'] = bronze_df_rn['timestamp'].apply(extract_date)
    load_to_db(df=bronze_df_rn, engine=engine, table_name='silver_turbines', schema=silver_schema)
    return

def gold(engine):
    silver_df = pd.read_sql('SELECT * FROM silver_turbines', con=engine)

    # Create Gold Anomaly table
    anomaly_df = anomaly_detection(df=silver_df)
    load_to_db(df=anomaly_df, engine=engine, table_name='gold_turbines', schema=gold_schema)

    # Create Gold Summary table
    statistics_df = statistics_table(engine=engine, table_name='silver_turbines')
    load_to_db(df=statistics_df, engine=engine, table_name='gold_turbines_summary', schema=gold_statistics_schema)
    return


def entrypoint():
    bronze(ENGINE)
    silver(ENGINE)
    gold(ENGINE)


if __name__ == "__main__":
    entrypoint()

