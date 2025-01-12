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

# DBTITLE 1,Ingest Raw Telemetry from Plants
from pyspark.sql.functions import *

# Read the raw telemetry data from the specified S3 path
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 16)
    .load("s3://db-gtm-industry-solutions/data/mfg/factory-optimization/incoming_sensors")
)

# Cast the 'body' column to string
df = df.withColumn("body", col("body").cast('string'))

# Write the stream to a Delta table in the specified schema
(df.writeStream
   .format("delta")
   .option("checkpointLocation", "/path/to/checkpoint/dir")
   .start("dbfs:/user/hive/warehouse/retail_roozbeh.db/OEE_bronze"))

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

silver_rules = {"positive_defective_parts":"defectivePartsMade >= 0",
                "positive_parts_made":"totalPartsMade >= 0",
                "positive_oil_level":"oilLevel >= 0",
                "expected_shifts": "shiftNumber >= 1 and shiftNumber <= 3"}

@dlt.table(comment = "Process telemetry from factory floor into Tables for Analytics",
                  table_properties={"pipelines.autoOptimize.managed": "true"})
@dlt.expect_all_or_drop(silver_rules)
def OEE_silver():
  return (dlt.read_stream('hive_metastore.retail_roozbeh.OEE_bronze')
             .withColumn("jsonData", from_json(col("body"), EEO_schema)) 
             .select("jsonData.applicationId", "jsonData.deviceId", "jsonData.messageProperties.*", "jsonData.telemetry.*") 
             .withColumn("messageTimestamp", to_timestamp(col("messageTimestamp"))))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Ingesting the Workforce dataset
# MAGIC
# MAGIC We'll then ingest the workforce data. Again, we'll be using the autoloader.
# MAGIC
# MAGIC *Note that we could have used any other input source (kafka etc.)*

# COMMAND ----------

@dlt.table(comment="count of workforce for a given shift read from delta")
def workforce_silver():
  return spark.read.format("delta").load("s3://db-gtm-industry-solutions/data/mfg/factory-optimization/workforce")

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

@dlt.create_table(name="OEE_gold",
                 comment="Aggregated equipment data from shop floor with additional KPI metrics like OEE",
                 spark_conf={"pipelines.trigger.interval" : "1 minute"},
                 table_properties={"pipelines.autoOptimize.managed": "true"})
@dlt.expect_all(gold_rules)
def create_agg_kpi_metrics():
  bus_agg = (dlt.read_stream("OEE_silver")
          .groupby(F.window(F.col("messageTimestamp"), "5 minutes"), "plantName", "productionLine", "shiftNumber")
          .agg(F.mean("systemDiskFreePercent").alias("avg_systemDiskFreePercent"),
               F.mean("oilLevel").alias("avg_oilLevel"), F.min("oilLevel").alias("min_oilLevel"),F.max("oilLevel").alias("max_oilLevel"),
               F.min("temperature").alias("min_temperature"),F.max("temperature").alias("max_temperature"),
               F.mean("temperature").alias("avg_temperature"),F.sum("totalPartsMade").alias("totalPartsMade"),
               F.sum("defectivePartsMade").alias("defectivePartsMade"),
               F.sum(F.when(F.col("machineHealth") == "Healthy", 1).otherwise(0)).alias("healthy_count"), 
               F.sum(F.when(F.col("machineHealth") == "Error", 1).otherwise(0)).alias("error_count"),
               F.sum(F.when(F.col("machineHealth") == "Warning", 1).otherwise(0)).alias("warning_count"), F.count("*").alias("total_count")
          )
          .withColumn("Availability", (F.col("healthy_count")-F.col("error_count"))*100/F.col("total_count"))
          .withColumn("Quality", (F.col("totalPartsMade")-F.col("defectivePartsMade"))*100/F.col("totalPartsMade"))
          .withColumn("Performance", (F.col("healthy_count")*100/(F.col("healthy_count")+F.col("error_count")+F.col("warning_count"))))
          .withColumn("OEE", (F.col("Availability")+ F.col("Quality")+F.col("Performance"))/3))
  return bus_agg.join(dlt.read("workforce_silver"), ["shiftNumber"], "inner")

# COMMAND ----------


