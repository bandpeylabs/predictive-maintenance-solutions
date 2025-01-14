# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Build Test/Train Datasets and Train Model
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC <img src="https://github.com/bandpeylabs/predictive-maintenance-solutions/blob/main/anomaly-detection/docs/screenshots/train_step.png?raw=true" width="50%">
# MAGIC
# MAGIC This notebook will label the Silver data, create training and test datasets from the labeled data, train a machine learning model, and deploy the model the MLflow model registry.

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ./utils/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
source_table = "silver"
target_table = "features"
checkpoint_location_target = f"{checkpoint_path}/dataset"

# COMMAND ----------

# DBTITLE 1,Read and label the Silver data
from pyspark.sql import functions as F

#Read the Silver Data
silver_df = spark.table(f"{database}.{source_table}")

#Uncomment to display silver_df
#display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import *

# Label the Silver data
labeled_df = (
  silver_df
    .withColumn("anomaly", when(col('sensor_1') > 80, 1.0)
                          .when(col('sensor_1') < 10, 1.0)
                          .when(col('sensor_1') > 65, round(rand(1)))
                          .when(col('sensor_1') < 25, round(rand(1)))
                          .otherwise(0.0)
                          .cast(FloatType()))
)

# Display the labeled data
display(labeled_df)

# COMMAND ----------

# DBTITLE 1,Save Feature to Delta Table
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

# Define window specifications
rollingWindowSpec = Window.partitionBy("device_id").orderBy("timestamp").rowsBetween(-1, 0)
lagWindowSpec = Window.partitionBy("device_id").orderBy("timestamp")

# Additional feature engineering
features_df = (
  labeled_df
    .withColumn("sensor_1_rolling_avg", F.avg("sensor_1").over(rollingWindowSpec).cast(FloatType()))
    .withColumn("sensor_2_rolling_avg", F.avg("sensor_2").over(rollingWindowSpec).cast(FloatType()))
    .withColumn("sensor_3_rolling_avg", F.avg("sensor_3").over(rollingWindowSpec).cast(FloatType()))
    .withColumn("sensor_1_diff", (F.col("sensor_1") - F.lag("sensor_1", 1).over(lagWindowSpec)).cast(FloatType()))
    .withColumn("sensor_2_diff", (F.col("sensor_2") - F.lag("sensor_2", 1).over(lagWindowSpec)).cast(FloatType()))
    .withColumn("sensor_3_diff", (F.col("sensor_3") - F.lag("sensor_3", 1).over(lagWindowSpec)).cast(FloatType()))
    .withColumn("hour_of_day", F.hour(F.col("timestamp")).cast(FloatType()))
    .withColumn("day_of_week", F.dayofweek(F.col("timestamp")).cast(FloatType()))
    .fillna(0.0)  # Fill null values with 0.0
)

# Save the features to a Delta table
features_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{database}.{target_table}")

# COMMAND ----------

# DBTITLE 1,Create Training and Test Datasets
import pandas as pd
import numpy as np
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import *
mlflow.spark.autolog()
mlflow.sklearn.autolog()

# Read data
data = features_df.toPandas().drop(["device_id", "timestamp"], axis=1)

train, test = train_test_split(data, test_size=0.30, random_state=206)
colLabel = 'anomaly'

# The predicted column is colLabel which is a scalar from [3, 9]
train_x = train.drop([colLabel], axis=1)
test_x = test.drop([colLabel], axis=1)
train_y = train[colLabel]
test_y = test[colLabel]

# COMMAND ----------

# DBTITLE 1,Design our pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline

def make_pipeline(max_depth, max_leaf_nodes):
  enc = OneHotEncoder(handle_unknown='ignore')
  model = DecisionTreeClassifier(max_depth=max_depth, max_leaf_nodes=max_leaf_nodes)
  pipeline = Pipeline(
    steps=[("preprocessor", enc), ("classifier", model)]
  )
  
  return pipeline

# COMMAND ----------

# DBTITLE 1,Initial Training Run
from sklearn.metrics import *
mlflow.spark.autolog()
mlflow.sklearn.autolog()

# Begin training run
max_depth = 4
max_leaf_nodes = 32

with mlflow.start_run(run_name="skl") as run:
    run_id = run.info.run_uuid
    pipeline = make_pipeline(max_depth, max_leaf_nodes)
    pipeline.fit(train_x, train_y)
    predictions = pipeline.predict(test_x)
    
#You can look at the experiment logging including parameters, metrics, recall curves, etc. by clicking the "experiment" link below or the MLflow Experiments icon in the right navigation pane

# COMMAND ----------

# DBTITLE 1,Hyper-Parameter Tuning with Hyperopt
from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope
import numpy as np

search_space = {
    'max_depth': hp.choice('max_depth', range(1,20)),
    'max_leaf_nodes': hp.choice('max_leaf_nodes', range(4,128))
}

def train_model(params):
  mlflow.sklearn.autolog()
  
  with mlflow.start_run(nested=True):
   
   # Fit, train, and score the model
    pipeline = make_pipeline(**params)
    pipeline.fit(train_x, train_y)
    predictions = pipeline.predict(test_x)

    return {'status': STATUS_OK, 'loss': f1_score(test_y, predictions)} #, 'params': model.get_params()}
  
with mlflow.start_run(run_name='sklearn_hyperopt') as run:
  
  best_params = fmin(
    fn = train_model,
    space = search_space,
    algo = tpe.suggest,
    max_evals = 20,
    trials = SparkTrials()
  )
  
run_id = run.info.run_uuid
experiment_id = run.info.experiment_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC You can look at the experiment logging including parameters, metrics, recall curves, etc. by clicking the "experiment" link above or the MLflow Experiments icon in the right navigation pane

# COMMAND ----------

# DBTITLE 1,Find and register the best model
from pyspark.sql.functions import *

experiment_Df = spark.read.format("mlflow-experiment").load(experiment_id)

#Find the best run based on F1 score
best_run = (
  experiment_Df
    .filter(
      experiment_Df.tags["mlflow.rootRunId"]==run_id)
    .orderBy(experiment_Df.metrics["training_f1_score"].desc())
    .limit(1)
    .first()['run_id']
)

# COMMAND ----------

import mlflow
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
model_uri = f"runs:/{best_run}/model"

#Register the model
model_details = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DBTITLE 1,Transition the model to "Production" stage in the registry
from mlflow import MlflowClient

client = MlflowClient()

# Set an alias for the model version
client.set_registered_model_alias(
    name=model_name,
    alias="Production",
    version=model_details.version
)
