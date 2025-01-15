# Databricks notebook source
# MAGIC %md
# MAGIC ### 3 Model Inference
# MAGIC
# MAGIC In this notebook, we'll take a detour from training models and discuss how to leverage the models we've built in a simple, easy to manage way. Note that consumers of our models don't need to care about swapping in new versions of our model or the dependencies that come with it, we get that automatically with MLflow!</br></br>
# MAGIC
# MAGIC As before, make sure to configure the same catalog and run the first two notebooks before you run this one

# COMMAND ----------

# DBTITLE 1,Import Config
from utils.iot_setup import get_config
config = get_config(spark, catalog='default')

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLflow Batch Inference

# COMMAND ----------

# DBTITLE 1,Make Predictions
import mlflow
mlflow.set_registry_uri("databricks-uc")

feature_data = spark.read.table(config['silver_features']).toPandas()
model_uri = f"models:/{config['catalog']}.{config['schema']}.{config['model_name']}@Production"
production_model = mlflow.pyfunc.load_model(model_uri)
feature_data['predictions'] = production_model.predict(feature_data)
display(feature_data)

# COMMAND ----------

# MAGIC %md
# MAGIC You can also load MLflow models to make predictions in parallel using Spark, as [demonstrated here](https://docs.databricks.com/en/mlflow/model-example.html) and as demonstrated on the artifacts tab of your MLflow UI

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLflow Streaming Inference

# COMMAND ----------

# MAGIC %md
# MAGIC How do we make predictions on real time streams of data? Same as before, but on a streaming dataframe. We'll put our logic in a function this time. The foreachBatch calls the function on each microbatch in our streaming dataframe

# COMMAND ----------

# DBTITLE 1,Define Function
feature_data_stream = spark.readStream.table(config['silver_features'])

def make_predictions(microbatch_df, batch_id):
    df_to_predict = microbatch_df.toPandas()
    df_to_predict['predictions'] = production_model.predict(df_to_predict) # we use the same model and function to make predictions!
    spark.createDataFrame(df_to_predict).write.mode('overwrite').option('mergeSchema', 'true').saveAsTable(config['predictions_table'])

# COMMAND ----------

# DBTITLE 1,Run Streaming Inference
(
  feature_data_stream.writeStream
  .format('delta')
  .option('checkpointLocation', config['checkpoint_location'])
  .foreachBatch(make_predictions) # run our prediction function on each microbatch
  .trigger(availableNow=True) # if you want to run in real time, comment out this line
  .queryName(f'stream_to_{config["predictions_table"]}') # use this for discoverability in the Spark UI
  .start()
).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks also supports real time model, feature, and function serving APIs. Check out <a href="https://docs.databricks.com/en/machine-learning/model-serving/index.html">our documentation</a> for more information on those topics
