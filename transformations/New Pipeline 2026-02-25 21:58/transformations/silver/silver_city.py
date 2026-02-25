# Import necessary Libaries
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Create Function to read the city data from Bronze layer
@dp.materialized_view(
    name="transportation.silver.city_view",
    comment="Silver layer for city data",
    # depends_on=["transportation.bronze.city"],
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "pipelines.autoOptimize.useV2": "true",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_data_silver():
    df_bronze = spark.read.table("city_view")
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingestion_date").alias("bronze_ingest_timestamp")
    )

    df_silver = (
        df_silver
            .withColumn("silver_ingest_timestamp", F.current_timestamp())
        )
    
    return df_silver

 
