# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Ingesting Sensor JSON payloads and saving them as our first table
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/oee_score/ooe_flow_1.png" width="600px"/>
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


