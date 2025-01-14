# Databricks notebook source
# MAGIC %md You may find this series of notebooks at https://github.com/databricks-industry-solutions/iot-anomaly-detection. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Predict Anomalous Events
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <img src="https://github.com/bandpeylabs/predictive-maintenance-solutions/blob/main/anomaly-detection/docs/screenshots/inference_step.png?raw=true" width="25%">
# MAGIC
# MAGIC This notebook will use the trained model to identify anomalous events.

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ./utils/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
source_table = "features"
target_table = "gold"
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read Silver Data

# COMMAND ----------

#Read Silver Data
silver_df = (
  spark.readStream
    .format("delta")
    .table(f"{database}.{source_table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create a function to featurize and make the prediction

# COMMAND ----------

import mlflow
import pyspark.sql.functions as F

# Feature and predict function
def predict_anomalies(data, epoch_id):
    # Load the model
    model_uri = f'models:/{model_name}@production'
    model_fct = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

    # Make the prediction
    prediction_df = data.withColumn('prediction', model_fct(*data.drop('timestamp', 'device_id').columns).getItem(0))
    
    # Clean up the output
    clean_pred_df = prediction_df.select(
        'device_id', 'timestamp', 'device_model', 'sensor_1', 'sensor_2', 'sensor_3', 'site_name',
        'anomaly', 'sensor_1_rolling_avg', 'sensor_2_rolling_avg', 'sensor_3_rolling_avg',
        'sensor_1_diff', 'sensor_2_diff', 'sensor_3_diff', 'hour_of_day', 'day_of_week', 'prediction'
    )
    
    # Write the output to a Gold Delta table
    clean_pred_df.write.format('delta').mode('append').option("mergeSchema", "true").saveAsTable(f"{database}.{target_table}")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Stream the predicted results using the function

# COMMAND ----------

# Stream predicted outputs
(
  silver_df
    .writeStream
    .foreachBatch(predict_anomalies)
    .trigger(once=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,Display our results
display(spark.table(f"{database}.{target_table}"))
