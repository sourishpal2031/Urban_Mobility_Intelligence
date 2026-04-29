# Databricks notebook source
import pandas as pd

files = [
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"}
]

for file in files:
    url = f"https://dlstreamingprojdev.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=rl&st=2026-04-28T20:21:20Z&se=2026-04-30T04:36:20Z&spr=https&sv=2025-11-05&sr=c&sig=ymGmeqXD7gPKKxwMn7ud6iT25k2FnySeefKWrJd4Y%2Bc%3D"

    df = pd.read_json(url)
    df_spark = spark.createDataFrame(df)

    table_name = f"streaming.bronze.{file['file']}"

    # FIRST LOAD ONLY → create table
    if not spark.catalog.tableExists(table_name):
        df_spark.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

    # FUTURE LOADS → append history for SCD2 testing
    else:
        df_spark.write.format("delta") \
            .mode("append") \
            .saveAsTable(table_name)

    print(f"Loaded {file['file']}")
    display(df_spark)

# COMMAND ----------

import pandas as pd

url = "https://dlstreamingprojdev.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=rl&st=2026-04-28T20:21:20Z&se=2026-04-30T04:36:20Z&spr=https&sv=2025-11-05&sr=c&sig=ymGmeqXD7gPKKxwMn7ud6iT25k2FnySeefKWrJd4Y%2Bc%3D"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)

table_name = "streaming.bronze.bulk_rides"

if not spark.catalog.tableExists(table_name):
    df_spark.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    print("Initial bulk_rides load completed")
else:
    print("bulk_rides already exists — skipped")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming.bronze.rides_raw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming.bronze.map_cities
# MAGIC LIMIT 5