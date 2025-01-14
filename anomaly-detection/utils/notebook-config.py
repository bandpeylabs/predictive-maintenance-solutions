# Databricks notebook source
# DBTITLE 1,Set database and streaming checkpoint
checkpoint_path = "dbfs:/FileStore/demos/anomaly-detection/checkpoints"
catalog = 'demos'
database = "anomaly_detection"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# dbutils.fs.rm(checkpoint_path, True) # resetting checkpoint - uncomment this out if you want to reset data for this accelerator 
# spark.sql(f"drop database if exists {database} cascade") # resetting database - comment this out if you want data to accumulate in tables for this accelerator over time

# COMMAND ----------

# DBTITLE 1,Database settings
spark.sql(f"create database if not exists {database}")

# COMMAND ----------

# DBTITLE 1,mlflow settings
import mlflow
model_name = "iot_anomaly_detection"
mlflow.set_experiment('/demos/iot_anomaly_detection')
