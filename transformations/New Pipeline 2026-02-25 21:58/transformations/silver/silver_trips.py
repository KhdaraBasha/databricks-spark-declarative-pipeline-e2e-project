# Import the required libraries
import pandas as pd
from datetime import datetime, timedelta
from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Create a function to load from broonze layer to silver layer
@dp.view(
    name="vw_trips_silver_stagging",
    comment= "Trsanformed trips data ready for CDC upsert"
    )
@dp.expect("valid_date", "year(business_date) >= 2020 and business_date IS NOT NULL")
@dp.expect("valid_id", "id IS NOT NULL")
@dp.expect("valid_driver_rating", "driver_rating between 1 and 10")
@dp.expect("valid_passenger_rating", "passenger_rating between 1 and 10")
def trips_data_silver():
    # Read the data from bronze layer
    df_bronze = spark.readStream.table("transportation.bronze.tb_trips_raw")

    df_silver = (
        df_bronze.select(
            F.col("trip_id").alias("id"),
            F.col("date").cast("date").alias("business_date"),
            F.col("city_id").alias("city_id"),
            F.col("passenger_type").alias("passenger_category"),
            F.col("distance_travelled_km").alias("distance_kms"),
            F.col("fare_amount").alias("sales_amt"),
            F.col("passenger_rating").alias("passenger_rating"),
            F.col("driver_rating").alias("driver_rating"),
            F.col("ingest_datetime").alias("bronze_ingest_datetime")
        ))

    df_silver = (
        df_silver.withColumn("silver_ingest_datetime", F.current_timestamp()))
   
    return df_silver

dp.create_streaming_table(
    name="transportation.silver.tb_trips_silver",
    comment="Trsanformed trips data ready for CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
     },
)

dp.create_auto_cdc_flow(
    target="transportation.silver.tb_trips_silver",
    source="vw_trips_silver_stagging",
    keys=["id"],
    sequence_by=F.col("silver_ingest_datetime"),
    stored_as_scd_type=1,
    except_column_list=[],
)