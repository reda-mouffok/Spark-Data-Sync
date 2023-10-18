import yaml
from pyspark.sql.functions import expr


def read_yaml(file_path):
    with open(file_path, 'r') as stream:
        try:
            # Converts yaml document to python object
            yaml_dict = yaml.safe_load(stream)
            return yaml_dict
        except yaml.YAMLError as e:
            print(e)


def get_dict_value(yaml_dict, key):
    if key in yaml_dict.keys():
        return yaml_dict[key]
    else:
        return False


def read_csv(spark, file_path):
    return spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(file_path)


def read_parquet(spark, file_path):
    return spark.read \
        .format("parquet") \
        .load(file_path)


def write_csv(df, file_path, mode):
    df.write \
        .format("csv") \
        .mode(mode) \
        .option("header", "true") \
        .save(file_path)


def write_parquet(df, file_path, mode):
    df.write \
        .format("parquet") \
        .mode(mode) \
        .save(file_path)


def union_by_adding_column(df1, df2):
    """
    This method aims to do an union on two dataframes `
    that not contains the same columns number or name.
    @param df1:  The first dataframe.
    @param df2:  The second dataframe.
    @return:  A dataframe containing the union of the two dataframe
    """
    cols = list(set(df1.columns + df2.columns))
    for col in cols:
        if col not in df1.columns:
            df1 = df1.withColumn(col, expr("null"))
        if col not in df2.columns:
            df2 = df2.withColumn(col, expr("null"))
    return df1.unionByName(df2)
