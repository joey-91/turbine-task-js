from sqlalchemy import Table, Column, Integer, Float, String, Boolean, MetaData

bronze_schema = Table(
    'bronze_turbines', MetaData(),
    Column('timestamp', String),
    Column('turbine_id', Integer),
    Column('wind_speed', Float),
    Column('wind_direction', Integer),
    Column('power_output', Float),

)

silver_schema = Table(
    'silver_turbines', MetaData(),
    Column('timestamp', String),
    Column('turbine_id', Integer),
    Column('wind_speed', Float),
    Column('wind_direction', Integer),
    Column('power_output', Float),
    Column('date', String)
)

gold_schema = bronze_schema = Table(
    'gold_turbines', MetaData(),
    Column('timestamp', String),
    Column('turbine_id', Integer),
    Column('wind_speed', Float),
    Column('wind_direction', Integer),
    Column('power_output', Float),
    Column('date', String),
    Column('avg_power_output', Float),
    Column('stddev_power_output', Float),
    Column('anomaly', Float)
)

gold_statistics_schema = bronze_schema = Table(
    'gold_summary', MetaData(),
    Column('turbine_id', Integer),
    Column('date', String),
    Column('avg_power_output', Float),
    Column('min_power_output', Float),
    Column('max_power_output', Float)
)