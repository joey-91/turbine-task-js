import sqlalchemy
import pandas as pd
import logging
from src.database import create_db, load_to_db
from src.data_ingestion import concat_files
from src.data_cleansing import remove_nulls, extract_date
from src.data_transformation import anomaly_detection, statistics_table
from src.schemas import bronze_schema, silver_schema, gold_schema, gold_statistics_schema

# logging to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# creating database
ENGINE = create_db(db_name='turbine_data.db')

def populate_bronze(engine):
    try:
        # Read raw data
        raw_df = concat_files(filepath='./data')
        logger.info(f"Loaded {len(raw_df)} rows of raw data")

        # Load Bronze table
        load_to_db(df=raw_df, engine=engine, table_name='bronze_turbines', schema=bronze_schema)
        logger.info("Successfully created bronze layer")

    except Exception as e:
        logger.error(f"Error in bronze layer: {str(e)}")
        raise

def populate_silver(engine):
    try:
        # Read bronze data
        bronze_df = pd.read_sql('SELECT * FROM bronze_turbines', con=engine)
        logger.info(f"Loaded {len(bronze_df)} rows from bronze layer")

        # Create silver table
        bronze_df_rn = remove_nulls(bronze_df)
        bronze_df_rn['date'] = bronze_df_rn['timestamp'].apply(extract_date)
        logger.info(f"Removed nulls, {len(bronze_df_rn)} rows remaining")

        # Load silver table
        load_to_db(df=bronze_df_rn, engine=engine, table_name='silver_turbines', schema=silver_schema)
        logger.info("Successfully created silver layer")

    except Exception as e:
        logger.error(f"Error in silver layer: {str(e)}")
        raise   

def populate_gold(engine):
    try:
        # Read from silver data
        silver_df = pd.read_sql('SELECT * FROM silver_turbines', con=engine)
        logger.info(f"Loaded {len(silver_df)} rows from silver layer")

        # Create & load Gold Anomaly table
        anomaly_df = anomaly_detection(df=silver_df)
        load_to_db(df=anomaly_df, engine=engine, table_name='gold_turbines', schema=gold_schema)
        logger.info("Successfully created gold table with anomaly detection")

        # Create & load Gold Summary table
        statistics_df = statistics_table(engine=engine, table_name='silver_turbines')
        load_to_db(df=statistics_df, engine=engine, table_name='gold_turbines_summary', schema=gold_statistics_schema)
        logger.info("Successfully created gold summary table")
    
    except Exception as e:
        logger.error(f"Error in gold layer: {str(e)}")
        raise


def entrypoint():
    try:
        logger.info("Starting data pipeline")
        populate_bronze(ENGINE)
        populate_silver(ENGINE)
        populate_gold(ENGINE)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    entrypoint()

