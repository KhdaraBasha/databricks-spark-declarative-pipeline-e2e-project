# Import necessary libraries
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *
from datetime import datetime

# Set Widgets
dbutils.widgets.text("source_path", "")

# Source Path
SOURCE_PATH = "s3://goodcabs-data-dp/data-store/city"

# The @dp.table decorator below tells Lakeflow Declarative Pipelines to create a materialized view
# based on the results returned by the city_data_bronze function. This materialized view is kept up to date
# automatically and is published to the catalog as a managed Delta table.
@dp.table(
    name="transportation.bronze.city_view", 
    comment="Bronze Table for City Data", 
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "source_path": SOURCE_PATH,
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
def city_data_bronze():
    # Step 1: Read CSV data from the source path
    # - Uses Spark's CSV reader to load data from the specified S3 path.
    # - Header is enabled to use column names from the file.
    # - Schema is inferred automatically.
    # - Permissive mode allows loading even if some records are corrupt.
    # - mergeSchema enables schema evolution.
    # - columnNameOfCorruptRecord captures corrupt records in a dedicated column.
    df = (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .option("mergeSchema", "true")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(SOURCE_PATH)
    )
    # Step 2: Add metadata columns
    # - Adds the file path from which each record was loaded using the _metadata.file_path column.
    # - Adds an ingestion timestamp to track when each record was processed.
    df = (
        df.withColumn("file_path", col("_metadata.file_path"))
          .withColumn("ingestion_date", current_timestamp())
    )
    # Step 3: Return the transformed DataFrame
    # - The DataFrame now includes all original columns, plus file_path and ingestion_date.
    return df