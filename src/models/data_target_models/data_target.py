from src.data_tools.data_tools import get_dict_value


class DataTarget:

    def __init__(self, spark_session, jdbc_connection, job_name, properties):
        self.spark = spark_session
        self.job_name = job_name
        self.jdbc_connection = jdbc_connection
        self.target_type = get_dict_value(properties, "targetType")
        self.table_name = get_dict_value(properties, "tableName")
        self.file_path = get_dict_value(properties, "filePath")
        self.source_key = get_dict_value(properties, "sourceKey")
        self.target_key = get_dict_value(properties, "targetKey")
        self.partitions_number = get_dict_value(properties, "partitionsNumber")
        self.partitions_column = get_dict_value(properties, "partitionsColumn")
        self.job_mode = properties["mode"]
        self.properties = properties

    def check_yaml_schema(self):
        pass

    def write_dataframe(self, df):
        pass
