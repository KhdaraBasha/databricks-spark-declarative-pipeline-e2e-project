# Import the required libraries for data processing and Lakeflow Declarative Pipelines
import pandas as pd
from datetime import datetime, timedelta
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Declare the source path for raw trips data stored in S3
SOURCE_PATH = "s3://goodcabs-data-dp/data-store/trips"

# Define a streaming table for ingesting raw trips data from S3 using Auto Loader
@dp.table(
    name="transportation.bronze.tb_trips_raw", 
    comment="Streaming ingestion of raw trips data with Auto Loader",
    table_properties={
        # Data quality and layer tags
        "quality": "bronze",  # Indicates bronze quality layer
        "layer": "bronze",    # Specifies the pipeline layer

        # Source metadata
        "source_format": "csv",  # Source file format
        "source_path": SOURCE_PATH,  # Source path in S3
        "source_type": "s3",  # Source type

        # Delta table optimizations and features
        "delta.enableChangeDataFeed": "true",  # Enable change data feed for CDC
        "delta.autoOptimize.optimizeWrite": "true",  # Optimize write operations
        "delta.autoOptimize.autoCompact": "true"  # Enable auto compaction
    }
)
def orders_bronze():
  # Step 1: Read streaming CSV files from S3 using Auto Loader with schema inference and rescue mode for schema evolution
  df = (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "s3://goodcabs-data-dp/data-store/trips/checkpoint")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )
  
  # Step 2: Rename the problematic column 'distance_travelled(km)' to 'distance_travelled_km' for compatibility
  df = (
        df.withColumnRenamed(
            "distance_travelled(km)",
             "distance_travelled_km"
        ))
  
  # Step 3: Add metadata columns for file name and ingestion timestamp to track data lineage and ingestion time
  df = (
      df.withColumn("file_name", F.col("_metadata.file_name"))
        .withColumn("ingest_datetime", F.current_timestamp())   
        )
  
  # Step 4: Return the transformed DataFrame to be written to the bronze streaming table
  return df