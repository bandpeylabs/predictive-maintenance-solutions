# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Parse/Transform the data from Bronze and load to Silver
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <img src="https://github.com/bandpeylabs/predictive-maintenance-solutions/blob/main/anomaly-detection/docs/screenshots/transform_step.png?raw=True" width="50%">
# MAGIC
# MAGIC
# MAGIC
# MAGIC This notebook will stream new events from the Bronze table, parse/transform them, and load them to a Delta table called "Silver".

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ./utils/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
source_table = "bronze"
target_table = "silver"
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Incrementally Read data from Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

bronze_df = (
  spark.readStream
    .format("delta")
    .table(f"{database}.{source_table}")
)

#Uncomment to view the bronze data
#display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse/Transform the Bronze data

# COMMAND ----------

# Define the schema for the DataFrame
schema = StructType([
  StructField("timestamp", IntegerType(), True),
  StructField("device_id", IntegerType(), True),
  StructField("device_model", StringType(), True),
  StructField("sensor_1", FloatType(), True),
  StructField("sensor_2", FloatType(), True),
  StructField("sensor_3", FloatType(), True),
  StructField("site_name", StringType(), True)
])

# Parse/Transform
transformed_df = (
  bronze_df
    .select(
      F.col("timestamp").cast("timestamp").alias("timestamp"),
      F.col("device_id").cast("integer").alias("device_id"),
      F.col("device_model").cast("string").alias("device_model"),
      F.col("sensor_1").cast("float").alias("sensor_1"),
      F.col("sensor_2").cast("float").alias("sensor_2"),
      F.col("sensor_3").cast("float").alias("sensor_3"),
      F.col("site_name").cast("string").alias("site_name")
    )
)

# Uncomment to display the transformed data
# display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write transformed data to Silver

# COMMAND ----------

(
  transformed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location_target)
    .trigger(once = True) # or use .trigger(processingTime='30 seconds') to continuously stream and feel free to modify the processing window
    .table(f"{database}.{target_table}")
)

# COMMAND ----------

#Display Silver Table
display(spark.table(f"{database}.{target_table}"))
