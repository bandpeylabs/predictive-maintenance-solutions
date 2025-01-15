import random
import datetime
import sys

def get_config(spark, catalog='default', schema='iot_distributed_pandas'):
    current_user = spark.sql('select current_user()').collect()[0][0].split('@')[0].replace('.', '_')
    username = spark.sql("SELECT current_user()").first()['current_user()'] 
    if not catalog:
      catalog = current_user
    schema = schema
    volume = f'/Volumes/{catalog}/{schema}/{current_user}_checkpoints'
    return {
        'current_user': current_user,
        'catalog': catalog,
        'schema' : schema,
        'volume' : volume,
        'bronze_table' : f'{catalog}.{schema}.{current_user}_sensor_bronze',
        'defect_table' : f'{catalog}.{schema}.{current_user}_defect_bronze',
        'silver_table' : f'{catalog}.{schema}.{current_user}_sensor_silver',
        'silver_features' : f'{catalog}.{schema}.{current_user}_features_table',
        'gold_table' : f'{catalog}.{schema}.{current_user}_sensor_gold',
        'predictions_table' : f'{catalog}.{schema}.{current_user}_predictions',
        'tuned_bronze_table' : f'{catalog}.{schema}.{current_user}_sensor_bronze_clustered',
        'csv_staging' : f'/{current_user}_iot_distributed_pandas/csv_staging',
        'checkpoint_location' : f'{volume}/checkpoints/',
        'train_table' : f'{volume}/train_table', 
        'test_table' : f'{volume}/test_table',  
        'log_path' : f'{volume}/pl_training_logger',
        'ckpt_path' : f'{volume}/pl_training_checkpoint',
        'pl_experiment_path' : f'/Users/{username}/distributed_pl',
        'ml_experiment_path' : f'/Users/{username}/mlflow_intro',
        'model_name' : f'device_defect_{current_user}'
    }

def reset_tables(spark, config, dbutils, drop_schema=False, create_schema=True):
    try:
      if drop_schema:
          spark.sql(f"drop schema if exists {config['catalog']}.{config['schema']} CASCADE")
      if create_schema:
          spark.sql(f"create schema {config['catalog']}.{config['schema']}")
      volume_fqp = '.'.join(config['volume'].split('/')[2:])
      spark.sql(f"create volume {volume_fqp}")
    except Exception as e:
        if 'NO_SUCH_CATALOG_EXCEPTION' in str(e):
            spark.sql(f'create catalog {config["catalog"]}')
            reset_tables(spark, config, dbutils)
        else:
            raise

dgconfig = {
    "shared": {
        "num_rows": 250000,
        "num_devices": 200,
        "start": datetime.datetime(2023, 1, 1, 0, 0, 0),
        "end": datetime.datetime(2023, 12, 31, 23, 59, 59),
        "frequency": 0.35,
        "amplitude": 1.2,
    },
    "timestamps": {
        "column_name": "timestamp",
        "minimum": 10,
        "maximum": 350,
    },
    "temperature": {
      "lifetime": {
        "column_name": "temperature",
        "noisy": 0.3,
        "trend": 0.1,
        "mean": 58,
        "std_dev": 17,
      },
      "trip": {
        "trend": -0.8,
        "noisy": 1,
      }
    },
    "air_pressure": {
      "column_name": "air_pressure",
      "depedent_on": "temperature",
      "min": 913,
      "max": 1113,
      "subtract": 15 
    },
    "lifetime": {
        "trend": 0.4,
        "noisy": 0.6,
    },
    "trip": {
        "trend": 0.2,
        "noisy": 1.2,
    },
}