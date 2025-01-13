# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Ingesting Sensor JSON payloads and saving them as our first table
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/bandpeylabs/predictive-maintenance-solutions/blob/main/factory-optimization-bi/docs/diagrams-process.png?raw=true" width="100%"/>
# MAGIC
# MAGIC Our raw data is being streamed from the device and sent to a blob storage. 
# MAGIC
# MAGIC Autoloader simplify this ingestion by allowing incremental processing, including schema inference, schema evolution while being able to scale to millions of incoming files. 
# MAGIC
# MAGIC Autoloader is available in __SQL & Python__ using the `cloud_files` function and can be used with a variety of format (json, csv, binary, avro...). It provides
# MAGIC
# MAGIC - Schema inference and evolution
# MAGIC - Scalability handling million of files
# MAGIC - Simplicity: just define your ingestion folder, Databricks take care of the rest!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the catalog
# MAGIC CREATE CATALOG IF NOT EXISTS demos;
# MAGIC
# MAGIC -- Create the schema within the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS demos.factory_optimization;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_METASTORE();

# COMMAND ----------

# Define the paths
schema_location = "dbfs:/FileStore/demos/factory_optimization/bronze/schema"
checkpoint_location = "dbfs:/FileStore/demos/factory_optimization/bronze/checkpoint"

# Create the directories
dbutils.fs.mkdirs(schema_location)
dbutils.fs.mkdirs(checkpoint_location)

# Print the paths to verify
print(f"Schema location: {schema_location}")
print(f"Checkpoint location: {checkpoint_location}")

# COMMAND ----------

# DBTITLE 1,Ingest Raw Telemetry from Plants
from pyspark.sql.functions import *

# Read the raw telemetry data from the specified S3 path
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 16)
    .option("cloudFiles.schemaLocation", schema_location)
    .load("s3://db-gtm-industry-solutions/data/mfg/factory-optimization/incoming_sensors")
)

# Cast the 'body' column to string
df = df.withColumn("body", col("body").cast('string'))

# Write the stream to a Delta table in the specified schema
(df.writeStream
   .format("delta")
   .option("mergeSchema", "true")
   .option("checkpointLocation", checkpoint_location)
   .table("demos.factory_optimization.bronze"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Silver layer: transform JSON data into tabular table
# MAGIC
# MAGIC The next step is to cleanup the data we received and extract the important field from the JSON data.
# MAGIC
# MAGIC This will increase performance and make the table schema easier to discover for external users, allowing to cleanup any missing data in the process.
# MAGIC
# MAGIC We'll be using Delta Live Table Expectations to enforce the quality in this table.
# MAGIC
# MAGIC In addition, let's enabled the managed autoOptimized property in DLT. The engine will take care of compaction out of the box.

# COMMAND ----------

#get the schema we want to extract from a json example
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
example_body = """{
  "applicationId":"3e9449fe-3df7-4d06-9375-5ee9eeb0891c",
  "deviceId":"Everett-BoltMachine-2",
  "messageProperties":{"iothub-connection-device-id":"Everett-BoltMachine-2","iothub-creation-time-utc":"2022-05-03T17:05:26-05:00","iothub-interface-id":""},
  "telemetry": {"batchNumber":23,"cpuLoad":3.03,"defectivePartsMade":2,"machineHealth":"Healthy","memoryFree":211895179,"memoryUsed":56540277,
   "messageTimestamp":"2022-05-03T22:05:26.402569Z","oilLevel":97.50000000000014,"plantName":"Everett","productionLine":"ProductionLine 2",
   "shiftNumber":3,"systemDiskFreePercent":75,"systemDiskUsedPercent":25,"temperature":89.5,"totalPartsMade":99}}"""
  
EEO_schema = spark.read.json(sc.parallelize([example_body])).schema

# COMMAND ----------

from pyspark.sql.functions import *

# Define the silver rules
silver_rules = {
    "positive_defective_parts": "defectivePartsMade >= 0",
    "positive_parts_made": "totalPartsMade >= 0",
    "positive_oil_level": "oilLevel >= 0",
    "expected_shifts": "shiftNumber >= 1 and shiftNumber <= 3"
}

# Read the bronze table
bronze_df = spark.readStream.table("demos.factory_optimization.bronze")

# Extract JSON data and apply schema
silver_df = (
    bronze_df
    .withColumn("jsonData", from_json(col("body"), EEO_schema))
    .select("jsonData.applicationId", "jsonData.deviceId", "jsonData.messageProperties.*", "jsonData.telemetry.*")
    .withColumn("messageTimestamp", to_timestamp(col("messageTimestamp")))
)

# Apply data quality rules
for rule_name, rule_condition in silver_rules.items():
    silver_df = silver_df.filter(expr(rule_condition))

# Write the stream to a Delta table in the specified schema
(silver_df.writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", "dbfs:/FileStore/demos/factory_optimization/silver/checkpoint")
    .table("demos.factory_optimization.silver"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ingesting the Workforce dataset
# MAGIC
# MAGIC We'll then ingest the workforce data. Again, we'll be using the autoloader.
# MAGIC
# MAGIC *Note that we could have used any other input source (kafka etc.)*

# COMMAND ----------

# Read the workforce data from the specified S3 path
workforce_df = spark.read.format("delta").load("s3://db-gtm-industry-solutions/data/mfg/factory-optimization/workforce")

# Write the workforce data to the silver table
(workforce_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("demos.factory_optimization.workforce_silver"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Gold layer: Calculate KPI metrics such as OEE, Availability, Quality and Performance
# MAGIC
# MAGIC We'll compute these metrics for all plants under in our command center leveraging structured streamings *Stateful Aggregation*. 
# MAGIC
# MAGIC We'll do a sliding window aggregation based on the time: we'll compute average / min / max stats for every 5min in a sliding window and output the results to our final table.
# MAGIC
# MAGIC Such aggregation can be really tricky to write. You have to handle late message, stop/start etc.
# MAGIC
# MAGIC But Databricks offer a full abstraction on top of that. Just write your query and the engine will take care of the rest.

# COMMAND ----------

gold_rules = {"warn_defective_parts":"defectivePartsMade < 35", 
              "warn_low_temperature":"min_temperature > 70", 
              "warn_low_oilLevel":"min_oilLevel > 60", 
              "warn_decrease_Quality": "Quality > 99", 
              "warn_decrease_OEE": "OEE > 99.3"}

# COMMAND ----------

import pyspark.sql.functions as F

# Read the silver table
silver_df = spark.readStream.table("demos.factory_optimization.silver")

# Add watermark to handle late data
silver_df = silver_df.withWatermark("messageTimestamp", "10 minutes")

# Aggregate KPI metrics
bus_agg = (
    silver_df
    .groupby(
        F.window(F.col("messageTimestamp"), "5 minutes"), 
        "plantName", 
        "productionLine", 
        "shiftNumber"
    )
    .agg(
        F.mean("systemDiskFreePercent").alias("avg_systemDiskFreePercent"),
        F.mean("oilLevel").alias("avg_oilLevel"),
        F.min("oilLevel").alias("min_oilLevel"),
        F.max("oilLevel").alias("max_oilLevel"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature"),
        F.mean("temperature").alias("avg_temperature"),
        F.sum("totalPartsMade").alias("totalPartsMade"),
        F.sum("defectivePartsMade").alias("defectivePartsMade"),
        F.sum(F.when(F.col("machineHealth") == "Healthy", 1).otherwise(0)).alias("healthy_count"),
        F.sum(F.when(F.col("machineHealth") == "Error", 1).otherwise(0)).alias("error_count"),
        F.sum(F.when(F.col("machineHealth") == "Warning", 1).otherwise(0)).alias("warning_count"),
        F.count("*").alias("total_count")
    )
    .withColumn("Availability", (F.col("healthy_count") - F.col("error_count")) * 100 / F.col("total_count"))
    .withColumn("Quality", (F.col("totalPartsMade") - F.col("defectivePartsMade")) * 100 / F.col("totalPartsMade"))
    .withColumn("Performance", (F.col("healthy_count") * 100 / (F.col("healthy_count") + F.col("error_count") + F.col("warning_count"))))
    .withColumn("OEE", (F.col("Availability") + F.col("Quality") + F.col("Performance")) / 3)
)

# Read the workforce silver table
workforce_df = spark.read.table("demos.factory_optimization.workforce_silver")

# Join with workforce data
result_df = bus_agg.join(workforce_df, ["shiftNumber"], "inner")

# Write the stream to a Delta table in the specified schema
(result_df.writeStream
    .format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", "dbfs:/FileStore/demos/factory_optimization/gold/checkpoint")
    .outputMode("append")
    .table("demos.factory_optimization.gold"))

# COMMAND ----------

# Function to stop all streaming queries 
def stop_all_streams():
    stream_count = len(spark.streams.active)
    if stream_count > 0:
        print(f"Stopping {stream_count} streams")
        for s in spark.streams.active:
            try:
                s.stop()
            except Exception as e:
                print(f"Error stopping stream: {e}")
        print("All streams stopped.")
    else:
        print("No active streams to stop.")

# Call the function to stop all streams
stop_all_streams()

# COMMAND ----------


