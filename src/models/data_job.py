from src.models.data_source_models.data_source_jdbc import DataSourceJDBC
from src.models.data_source_models.data_source_csv import DataSourceCSV
from src.models.data_source_models.data_source_parquet import DataSourceParquet
from src.models.data_source_models.data_source import DataSource
from src.models.data_target_models.data_target import DataTarget
from src.models.data_target_models.data_target_jdbc import DataTargetJDBC
from src.models.data_target_models.data_target_csv import DataTargetCSV
from src.models.data_target_models.data_target_parquet import DataTargetParquet


class DataJob:
    def __init__(self, spark_session, jdbc_connection, job_name, properties):
        self.job_name = job_name
        self.data_source = self.get_source_instance(
            DataSource(spark_session,
                       jdbc_connection,
                       job_name,
                       properties["source"]
                       )
        )
        self.data_target = self.get_target_instance(
            DataTarget(spark_session,
                       jdbc_connection,
                       job_name,
                       properties["target"]
                       )
        )
        self.check_yaml_schema()

    # TODO add JSON XML AND HIVE and Kafka queue SOURCE
    def get_source_instance(self, data_source):
        if data_source.sourceType == 'csv':
            return DataSourceCSV(data_source.spark,
                                 data_source.jdbc_connection,
                                 data_source.jobName,
                                 data_source.properties
                                 )
        elif data_source.sourceType == 'parquet':
            return DataSourceParquet(data_source.spark,
                                     data_source.jdbc_connection,
                                     data_source.jobName,
                                     data_source.properties
                                     )
        elif data_source.sourceType in ('oracle', 'postgres'):
            return DataSourceJDBC(data_source.spark,
                                  data_source.jdbc_connection,
                                  data_source.jobName,
                                  data_source.properties
                                  )
        else:
            raise ValueError(
                data_source.jobName + " : sourceType attribute should contain CSV, Parquet, Hive, Oracle, Postgres"
            )

    def get_target_instance(self, data_target):
        if data_target.target_type == 'csv':
            return DataTargetCSV(data_target.spark,
                                 data_target.jdbc_connection,
                                 data_target.job_name,
                                 data_target.properties
                                 )
        elif data_target.target_type == 'parquet':
            return DataTargetParquet(data_target.spark,
                                     data_target.jdbc_connection,
                                     data_target.job_name,
                                     data_target.properties
                                     )
        elif data_target.target_type in ('oracle', 'postgres'):
            return DataTargetJDBC(data_target.spark,
                                  data_target.jdbc_connection,
                                  data_target.job_name,
                                  data_target.properties
                                  )
        else:
            raise ValueError(data_target.job_name + " : targetType should contain CSV, Parquet, Hive, Oracle, Postgres")

    def check_yaml_schema(self):
        self.data_source.check_yaml_schema()
        self.data_target.check_yaml_schema()

    def execute_job(self):
        df = self.data_source.read_dataframe()
        if df is not None:
            df = self.data_source.select_columns(df)
            df = self.data_source.filter_rows(df)
            df = self.data_source.rename_columns(df)
            self.data_target.write_dataframe(df)
