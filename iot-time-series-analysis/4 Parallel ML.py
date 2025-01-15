# Databricks notebook source
# MAGIC %md
# MAGIC ### 4 Parallel Pandas ML Techniques
# MAGIC
# MAGIC Finally, the crux of what we set out to do - in this notebook, we'll tour some approaches for distributing an end-to-end ML pipeline that uses all the libraries we're used to. While some of these implementation details, could be handled by out of the box Databricks tools such as Feature Store, we hope the various examples it contains provide valuable patterns and insights into how to scale your ML Pipelines.

# COMMAND ----------

# DBTITLE 1,Import Config
from utils.iot_setup import get_config
config = get_config(spark, catalog='default')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pandas + Spark
# MAGIC So far we've used Pandas to run some single core, single threaded transformations on our data. If our data volume grows, we may want to run processes in parallel instead. Spark offers several approaches for applying familiar Pandas logic on top of the parallelism of Spark, including:
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html">Pyspark Pandas</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.applyInPandas.html">Apply In Pandas</a>
# MAGIC - <a href="https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.pandas_udf.html">Pandas UDFs</a>
# MAGIC
# MAGIC First, let's use the same feature logic from the _1 Data Exploration_ notebook, but this time using Pyspark Pandas. This will scale out to all the cores and nodes on our Databricks cluster, as opposed to traditional pandas which is single node and will encounter OOM errors at larger scales. Spark also uses the entire cluster, while Pandas will leave much of a Spark cluster unused - and you can see this for yourself in the Ganglia UI or the "Metrics" tab on your Databricks cluster configuration page. We'll also put the logic in a function so we can re-use it and test the logic more easily

# COMMAND ----------

import pyspark.pandas as ps

features_ps = spark.read.table(config['bronze_table']).orderBy('timestamp').pandas_api()

def ohe_encoding(psdf):
    encoded_factory = ps.get_dummies(psdf['factory_id'], prefix='ohe')
    encoded_model = ps.get_dummies(psdf['model_id'], prefix='ohe')
    psdf = ps.concat([psdf.drop('factory_id', axis=1).drop('model_id', axis=1), encoded_factory, encoded_model], axis=1)
    psdf = psdf.drop('timestamp', axis=1)
    return psdf 

features_ps = ohe_encoding(features_ps)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's try the applyinpandas approach to run our rolling mean temperature Pandas transformations in parallel for each of the trips in our dataset

# COMMAND ----------

# DBTITLE 1,Add Temp Features
import pandas as pd

def add_rolling_temp(pdf: pd.DataFrame) -> pd.DataFrame: # these type hints tell the compiler the input/output format
    pdf['rolling_mean_temp'] = pdf['temperature'].ewm(5).mean()
    pdf['temp_difference'] = pdf['rolling_mean_temp'] - pdf['temperature']
    pdf = pdf.fillna(method='ffill').fillna(0)
    return pdf

rolling_temp_schema = '''
    device_id string, trip_id int, airflow_rate double, rotation_speed double, air_pressure double, 
    temperature double, delay float, density float, defect float, ohe_A06 double,
    ohe_AeroGlider4150 double, ohe_SkyBolt2 double, ohe_EcoJet2000 double, ohe_C04 double, ohe_D18 double, 
    ohe_EcoJet3000 double, ohe_FlyForceX550 double, ohe_J15 double, ohe_JetLift7000 double, ohe_MightyWing1100 double, 
    ohe_SkyBolt250 double, ohe_SkyJet234 double, ohe_SkyJet334 double, ohe_T10 double, ohe_EcoJet1000 double, 
    ohe_TurboFan3200 double, ohe_SkyBolt1 double, ohe_SkyJet134 double, rolling_mean_temp double, temp_difference double
'''

# Translate the dataframe back to Spark and apply our pandas function in parallel
features_spark = features_ps.to_spark()
features_temp = features_spark.groupBy('device_id', 'trip_id').applyInPandas(add_rolling_temp, rolling_temp_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's do the same, but by device for the rolling density metric

# COMMAND ----------

# DBTITLE 1,Add Trip Features
def add_rolling_density(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['rolling_mean_density'] = pdf['density'].shift(1).ewm(span=600).mean()
    pdf = pdf.fillna(method='ffill').fillna(0)
    return pdf

rolling_density_schema = '''
    device_id string, trip_id int, airflow_rate double, rotation_speed double, air_pressure double, 
    temperature double, delay float, density float, defect float, rolling_mean_temp double, 
    temp_difference double, rolling_mean_density double, ohe_A06 double, ohe_AeroGlider4150 double,
    ohe_SkyBolt2 double, ohe_EcoJet2000 double, ohe_C04 double, ohe_D18 double, ohe_EcoJet3000 double,
    ohe_FlyForceX550 double, ohe_J15 double, ohe_JetLift7000 double, ohe_MightyWing1100 double, ohe_SkyBolt250 double,
    ohe_SkyJet234 double, ohe_SkyJet334 double, ohe_T10 double, ohe_EcoJet1000 double, ohe_TurboFan3200 double,
    ohe_SkyBolt1 double, ohe_SkyJet134 double
'''

features_density = features_temp.groupBy('device_id').applyInPandas(add_rolling_density, rolling_density_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC If you chart the temperature for defects vs non-defects, you can see that temperature has a significant impact on defect rate. Currently we're using a rolling mean to provide our model more informative temperature features. But what if we could integrate a forward looking temperature prediction into the features?
# MAGIC
# MAGIC Let's train an ARIMA model to predict the next temperature that will occur using another method of parallelizing pandas operations, a Pandas UDF

# COMMAND ----------

# DBTITLE 1,Add Arima Forecast
from pyspark.sql.functions import pandas_udf, lit
from statsmodels.tsa.arima.model import ARIMA
import mlflow

# This is a neat programming trick called a closure which allows us to parameterize a function
# which wouldn't otherwise accept parameters
def create_forecast_arima(order):
    @pandas_udf("double")
    def forecast_arima(temperature: pd.Series) -> pd.Series:
        mlflow.sklearn.autolog(disable=True)
        model = ARIMA(temperature, order=order)
        model_fit = model.fit()
        return model_fit.predict()
    return forecast_arima

# Minimal Spark code - just select one column and add another. We can still use Pandas for our logic
forecast_arima = create_forecast_arima((1, 2, 4))
features_arima = features_density.withColumn('predicted_temp', forecast_arima('temperature'))

# COMMAND ----------

# DBTITLE 1,Evaluate ARIMA
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(predictionCol='predicted_temp', labelCol='temperature', metricName='rmse')
rmse = evaluator.evaluate(features_arima)
rmse

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hyperparameter tuning
# MAGIC Now we've seen how to run ARIMA in parallel on a large dataset, but we haven't determined which hyperparameters (the "order" parameter) gives the best ARIMA model for our use case. We can explore the correct hyperparameters by using hyperopt, a framework where we can minimize the output of some function given a parameter space to explore as input. In our case, we'll use rmse as the output of our objective function, and use hyperopt to automatically and intelligently explore many values for the "order" hyperparameters as the input to our objective function.

# COMMAND ----------

# DBTITLE 1,Define Objective Function
tune_arima_features = (
    spark.read.table(config['bronze_table'])
    .orderBy('timestamp')
    .select('temperature')
    .where('model_id="SkyJet334"')
)

# Define objective function to minimize
def objective(params):
    order = (params['p'], params['d'], params['q'])
    forecast_arima = create_forecast_arima(order)
    temp_predictions = (
        tune_arima_features.dropna()
        .select('temperature')
        .withColumn('predicted_temp', forecast_arima('temperature'))
    )
    return evaluator.evaluate(temp_predictions)

# Test two runs of the objective function with different parameters. Lower is better on the rmse evaluator
print(objective({'p': 1, 'd': 1, 'q': 1}), objective({'p': 1, 'd': 2, 'q': 0}))

# COMMAND ----------

# DBTITLE 1,Hyperparameter Tune ARIMA
from hyperopt import fmin, tpe, hp # , SparkTrials

# Define search space. Many possibilities, but Hyperopt identifies the best combinations to try
search_space = {'p': hp.quniform('p', 0, 2, 1),
                'd': hp.quniform('d', 0, 3, 1),
                'q': hp.quniform('q', 0, 4, 1)}

# Run intelligent hyperparameter search over the search space
# You can use trials=SparkTrials() if your code isn't already distributed
# This may take a few minutes - you can reduce max_evals to finish faster
argmin = fmin(fn=objective, space=search_space, algo=tpe.suggest, max_evals=16)
print('Optimal hyperparameters: ', argmin)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's finally train a new version of a classifier model from the _2 Model Experimentation_ notebook using the ARIMA model's output as a feature. We'll get back to using the full dataset soon

# COMMAND ----------

# DBTITLE 1,Train New Defect Model
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import recall_score

def train_with_arima(pdf: pd.DataFrame):
    X_train = pdf.drop('defect', axis=1)
    y_train = pdf['defect']
    rf = RandomForestClassifier(n_estimators=100) # We could run hyperopt for these hyperparameters too!
    rf.fit(X_train, y_train)
    return rf

mlflow.set_experiment(config['ml_experiment_path'])
mlflow.sklearn.autolog()
with mlflow.start_run() as run:
    rf_model = train_with_arima(features_arima.where('device_id = 1').toPandas())
mlflow.sklearn.autolog(disable=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom MLflow Model
# MAGIC Sometimes, logging models from default libraries doesn't cut it. For these cases we can use an MLflow [PythonModel](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#class-based-model) to define custom logic that provides value on top of standard out of the box models. For example, by logging the feature logic along with our model we mitigate a lot of potential headaches in productionization, such as online/offline skew. In this example, ensuring that our ARIMA model gets the appropriate optimal_order parameter also means we can create features in the same way our model was trained on. We can consider this pattern as a useful example for distributing existing code, but for new use cases please note the robust functionality provided by [Feature Store](https://docs.databricks.com/en/machine-learning/feature-store/index.html) and [MLops Stacks](https://docs.databricks.com/en/machine-learning/mlops/mlops-stacks.html). </br></br>
# MAGIC
# MAGIC Let's tie everything together from this notebook and log it all as a custom model in MLflow and use it to generate features and make predictions

# COMMAND ----------

# DBTITLE 1,Define Custom Model
class FeaturesAndPredictionModel(mlflow.pyfunc.PythonModel):
    def __init__(self, defect_model=None, order=(1, 1, 1), ohe_list=[]):
        self.defect_model = defect_model
        self.order = order
        self.ohe_list = ohe_list
    
    def ohe_encoding(self, pdf: pd.DataFrame) -> pd.DataFrame:
        encoded_factory = pd.get_dummies(pdf['factory_id'], prefix='ohe')
        encoded_model = pd.get_dummies(pdf['model_id'], prefix='ohe')
        pdf = pd.concat([pdf.drop('factory_id', axis=1).drop('model_id', axis=1), encoded_factory, encoded_model], axis=1)
        pdf = pdf.drop('timestamp', axis=1)
        return pdf

    def add_missing_ohes(self, pdf: pd.DataFrame) -> pd.DataFrame:
        ohes_to_add = [column for column in self.ohe_list if column not in pdf.columns]
        for col in ohes_to_add:
            pdf[col] = 0
        return pdf

    def add_rolling_temp(self, pdf: pd.DataFrame) -> pd.DataFrame:
        pdf['rolling_mean_temp'] = pdf['temperature'].ewm(5).mean()
        pdf['temp_difference'] = pdf['rolling_mean_temp'] - pdf['temperature']
        pdf = pdf.fillna(method='ffill').fillna(0)
        return pdf

    def add_rolling_density(self, pdf: pd.DataFrame) -> pd.DataFrame:
        pdf['rolling_mean_density'] = pdf['density'].shift(1).ewm(span=600).mean()
        pdf = pdf.fillna(method='ffill').fillna(0)
        return pdf
    
    def forecast_arima(self, pdf: pd.DataFrame) -> pd.DataFrame:
        mlflow.autolog(disable=True)
        model = ARIMA(pdf.temperature, order=self.order)
        model_fit = model.fit()
        pdf['predicted_temp'] = model_fit.predict()
        return pdf

    def generate_features(self, pdf: pd.DataFrame) -> pd.DataFrame:
        features = self.ohe_encoding(pdf)
        features = self.add_missing_ohes(features)
        features = self.add_rolling_temp(features)
        features = self.add_rolling_density(features)
        features = self.forecast_arima(features)
        return features
    
    def train_model(self, pdf: pd.DataFrame):
        X_train = pdf.drop('defect', axis=1)
        y_train = pdf['defect']
        rf = RandomForestClassifier(n_estimators=100) # We could run hyperopt for these hyperparameters too!
        rf.fit(X_train, y_train)
        return rf

    def predict(self, context, model_input):
        output = self.generate_features(model_input)
        if self.defect_model: # If there's a model, return predictions. Otherwise, just return generated features
            features = output[self.defect_model.feature_names_in_]
            output['prediction'] = self.defect_model.predict(features)
        return output 

# COMMAND ----------

# DBTITLE 1,Test Feature Generation
ohe_list = [column for column in features_arima.columns if 'ohe' in column]
optimal_order = (int(argmin['p']), int(argmin['d']), int(argmin['q']))
combo_model = FeaturesAndPredictionModel(None, optimal_order, ohe_list)

raw = spark.read.table(config['bronze_table']).limit(100).toPandas()
features = combo_model.generate_features(raw)
model = combo_model.train_model(features)
combo_model.defect_model = model

with mlflow.start_run() as run:
    mlflow.pyfunc.log_model('combo_model', python_model=combo_model, input_example=raw) 

custom_model = mlflow.pyfunc.load_model(f'runs:/{run.info.run_id}/combo_model')

# COMMAND ----------

# MAGIC %md
# MAGIC We could also use a [Spark UDF](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.spark_udf) to call our model in parallel, like what's automatically shown in the MLflow Run Artifacts Tab, or use the distributed Pandas approaches from above to distribute the feature generation in the exact same way. 
# MAGIC
# MAGIC # Nested MLflow models
# MAGIC If you try displaying our dataset and visualize the fault rate per model_id, you'll notice that the defect rates are different for each model_id - maybe we should consider training different ML models to make sure our predictions are aligned to the factors at play for each type of device (model_id) rather than making a very generalized ML model for all model_ids?
# MAGIC
# MAGIC This is a great scenario for our experimentation to be nested (and is why we didn't worry about parallelizing the custom model above). We want to create a different ML model for each type of engine to account for their different relationships to our features. Let's try logging multiple runs within a parent run, while each model is trained in parallel.

# COMMAND ----------

# DBTITLE 1,Create Nested Models
def single_model_run(pdf: pd.DataFrame) -> pd.DataFrame:
    run_id = pdf["run_id"].iloc[0]
    model_id = pdf["model_id"].iloc[0]
    n_used = pdf.shape[0]
    with mlflow.start_run(run_id=run_id) as outer_run:  # Set the top level run
        experiment_id = outer_run.info.experiment_id    # and nest the inner runs
        with mlflow.start_run(run_name=model_id, nested=True, experiment_id=experiment_id) as inner_run:
            model_specific_ml = FeaturesAndPredictionModel(None, optimal_order, ohe_list)
            features = model_specific_ml.generate_features(pdf.drop('run_id', axis=1))
            model_specific_ml.defect_model = model_specific_ml.train_model(features)
            mlflow.pyfunc.log_model(f'combo_model_{model_id}', python_model=model_specific_ml, input_example=features.head()) 
            mlflow.set_tag('model_id', model_id)
            return_df = pd.DataFrame([[model_id, inner_run.info.run_id, n_used]], columns=["model_id", "run_id", "n_used"])
    return return_df

train_return_schema = "model_id string, run_id string, n_used int"

bronze_df = spark.read.table(config['bronze_table'])

with mlflow.start_run(run_name="Device Specific Models") as run:
    model_info = (
        bronze_df
        .withColumn("run_id", lit(run.info.run_id)) # Pass in run_id
        .groupby("model_id")
        .applyInPandas(single_model_run, train_return_schema)
    )
model_info.display()

# COMMAND ----------

# DBTITLE 1,Create Delegating Model
from mlflow.pyfunc import PythonModel

class OriginDelegatingModel(PythonModel):
    def __init__(self, model_map):
        self.model_map = model_map

    def predict_for_model(self, model_group):
        model_id = model_group.iloc[0]['model_id']
        ml_model = self.model_map.get(model_id)
        return ml_model.predict(model_group)

    def predict(self, context, model_input):
        return model_input.groupby("model_id").apply(self.predict_for_model)

model_map = {row['model_id']: mlflow.pyfunc.load_model(f'runs:/{row["run_id"]}/combo_model_{row["model_id"]}')
             for row in model_info.collect()}
inference_model = OriginDelegatingModel(model_map)
delegated_predictions = inference_model.predict(None, bronze_df.drop('defect').sample(.01).toPandas())
display(delegated_predictions)

# COMMAND ----------

# DBTITLE 1,Log Final Model
with mlflow.start_run():
    model = OriginDelegatingModel(model_map)
    mlflow.pyfunc.log_model("model", python_model=inference_model)

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations on making it to the end of the Distributed Pandas for IOT Anomaly Detection notebooks! We've covered a lot:
# MAGIC - using more traditional pandas-oriented ML skillsets on Databricks
# MAGIC - model experimentation and registry with MLflow
# MAGIC - batch and streaming inference
# MAGIC - parallelizing pandas operations
# MAGIC - hyperparameter tuning
# MAGIC - custom MLflow models
# MAGIC </br>Armed with these tools, you should now feel more confident in scaling standard ML workflows
