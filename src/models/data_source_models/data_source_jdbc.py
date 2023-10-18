from src.data_tools.data_tools import read_csv
from src.models.data_source_models.data_source import DataSource


class DataSourceJDBC(DataSource):

    def read_dataframe(self):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_connection.get_url(self.sourceType)) \
            .option("dbtable", self.tableName) \
            .option("user", self.jdbc_connection.get_user(self.sourceType)) \
            .option("password", self.jdbc_connection.get_password(self.sourceType)) \
            .option("numPartitions", "10") \
            .option("driver", self.jdbc_connection.get_driver_type(self.sourceType)) \
            .load()

    def check_yaml_schema(self):
        if not self.tableName:
            raise Exception(self.jobName + " : " + self.sourceType + " sourceType should contain tableName attribute")
