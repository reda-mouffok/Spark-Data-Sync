from src.data_tools.data_tools import get_dict_value


class DataSource:

    def __init__(self, spark_session, jdbc_connection, job_name, properties):
        self.spark = spark_session
        self.jobName = job_name
        self.jdbc_connection = jdbc_connection
        self.sourceType = get_dict_value(properties, "sourceType")
        self.tableName = get_dict_value(properties, "tableName")
        self.filePath = get_dict_value(properties, "filePath")
        self.key = get_dict_value(properties, "key")
        self.column = get_dict_value(properties, "column")
        self.filter = get_dict_value(properties, "filter")
        self.renamed_column = get_dict_value(properties, "renamedColumn")
        self.read_tolerance = get_dict_value(properties, "readTolerance")
        self.properties = properties

    def check_yaml_schema(self):
        pass

    def read_dataframe(self):
        pass

    def select_columns(self, df):
        if self.column:
            return df.select(*self.column)
        else:
            return df

    def filter_rows(self, df):
        if self.filter:
            return df.filter(self.filter)
        else:
            return df

    def rename_columns(self, df):
        if self.renamed_column:
            for [oldName, newName] in self.renamed_column:
                df = df.withColumnRenamed(oldName, newName)
                return df
        else:
            return df
