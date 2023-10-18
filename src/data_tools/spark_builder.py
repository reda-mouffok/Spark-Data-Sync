import yaml
from pyspark.sql import SparkSession
from pyspark import SparkConf

from src.data_tools.data_tools import read_yaml


def create_spark_session(file_path):

    # Set conf variables
    spark_config = SparkConf()
    data = read_yaml(file_path)
    for key, value in data.items():
        spark_config.set(key, value)
    try:
        # Create the Spark Session
        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        return spark
    except Exception as spark_error:
        print(spark_error)
