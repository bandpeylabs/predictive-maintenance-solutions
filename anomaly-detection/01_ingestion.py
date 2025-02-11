# Databricks notebook source
# MAGIC %md 
# MAGIC #Real-time Monitoring and Anomaly Detection on Streaming IoT Pipelines in Manufacturing
# MAGIC
# MAGIC Manufacturers today are grappling with IoT data issues due to high costs, security headaches, and connectivity failures. Instead of innovating, they end up wasting time and money just trying to keep things running. Our real-time anomaly detection solution cuts through these problems with a platform that’s reliable and secure, capable of handling and transforming IoT data at massive scales. It builds analytics and AI assets on that data, serving them exactly where needed.
# MAGIC
# MAGIC We’ll show you how to set up a streaming pipeline for IoT data, train a machine learning model, and use it to predict new IoT data anomalies.
# MAGIC
# MAGIC This setup pulls data from an Azure Event Hub stream—a fancy way of saying it uses a distributed event streaming message bus that combines queuing and publish-subscribe tech, working seamlessly with Kafka connectors. Thanks to the Kafka connectors bundled within the Databricks runtime, you can persist Event Hub streams into Delta Lakehouse with minimal fuss. From there, unleash advanced analytics or machine learning algorithms on your data.
# MAGIC
# MAGIC <p></p>
# MAGIC
# MAGIC <img src="https://github.com/bandpeylabs/predictive-maintenance-solutions/blob/main/anomaly-detection/docs/diagrams/diagrams-target-architecture.png?raw=true" width=100%/>
# MAGIC
# MAGIC <p></p>
# MAGIC
# MAGIC ## Stream the Data from Azure Event Hub into a Bronze Delta Table
# MAGIC
# MAGIC <p></p>
# MAGIC <center><img src="https://github.com/bandpeylabs/predictive-maintenance-solutions/blob/main/anomaly-detection/docs/diagrams/diagrams-ingest.png?raw=true" width="30%"></center>
# MAGIC
# MAGIC
# MAGIC This notebook will read the IoT data from Azure Event Hub and put it into a Delta Lake table called "Bronze".

# COMMAND ----------

catalog_name = 'demos'
schema_name = 'anomaly_detection'
spark.sql(f"USE CATALOG {catalog_name}")

# Use the variable in the SQL command
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ./utils/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
# define target table for this notebook
target_table = "bronze"
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

# spark.sql(f"drop database if exists {database} cascade") # uncomment if you want to reinitialize the accelerator database

# COMMAND ----------

# DBTITLE 1,Ingest Raw Data from Kafka
# Read the stream from the specified table
streaming_df = spark.readStream.table("demos.anomaly_detection.iot_stream")

# Display the streaming DataFrame
display(streaming_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Read streams from `iot_stream` and write to bronze Delta table
# MAGIC
# MAGIC Here we generate some data. For real-world use cases, if there is a Kafka topic with data continuously arriving, you can skip the following data generation step.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

query = (
  streaming_df
    .withColumn("parsedValue_sensor_1", F.col("sensor_1").cast("float"))
    .withColumn("parsedValue_sensor_2", F.col("sensor_2").cast("float"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location_target)
    .trigger(processingTime='30 seconds')  # Use `.trigger(availableNow=True)` if you do NOT want to run the stream continuously, only to process available data since the last time it ran
    .toTable(f"{database}.{target_table}")
)

# COMMAND ----------

#Display records from the Bronze table
display(spark.table(f"{database}.{target_table}"))
