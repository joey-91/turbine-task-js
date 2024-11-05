import pandas as pd
from sqlalchemy.engine import Engine


def statistics_table(engine: Engine, table_name: str):
    """
    Aggregates data by turbine_id & date, taking the max, min & avg for each value

    Parameters:
        table_name (str): SQL table
        engine (SQLLite engine): Our SQL database instance

    Returns:
        pd.DataFrame: A DataFrame with all null values removed.
    """

    group_by_query = f'''
            SELECT
                turbine_id,
                date,
                ROUND(AVG(power_output),2) as avg_power_output,
                ROUND(MIN(power_output),2) as min_power_output,
                ROUND(MAX(power_output),2) as max_power_output

            FROM 
                {table_name}
            GROUP BY
                turbine_id,
                date

        '''

    return pd.read_sql(group_by_query, con=engine)

def anomaly_detection(df: pd.DataFrame):
    """
    Aggregates data by turbine_id & date, taking the std deviation & mean of each grouping, to calculate whether anomalous

    Parameters:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: A DataFrame with all null values removed.
    """

    # Grouping by turbine & date to calculate grouped statistics without aggregating
    grouped = df.groupby(['turbine_id', 'date'])['power_output']

    # calculating mean & std dev
    df['avg_power_output'] = round(grouped.transform('mean'),2)
    df['stddev_power_output'] = round(grouped.transform('std'),2)

    # Check if each power_output is within 2 standard deviations of the mean
    df['anomaly'] = df.apply(
        lambda x: abs(x['power_output'] - x['avg_power_output']) > 2 * x['stddev_power_output'], axis=1
        )

    return df
