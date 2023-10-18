from src.data_tools.data_tools import write_parquet
from src.models.data_target_models.data_target import DataTarget


class DataTargetParquet(DataTarget):

    def write_dataframe(self, df):
        if self.partitions_number:
            df = df.repartition(self.partitions_number)

        dataframe_writer = df.write \
            .format("parquet") \
            .mode(self.job_mode)

        if self.partitions_column:
            dataframe_writer.partitionBy(*self.partitions_column)

        dataframe_writer.save(self.file_path)

    def check_yaml_schema(self):
        if not self.file_path:
            raise Exception(self.job_name + " : Parquet targetType should contain filePath attribute")
        if self.job_mode not in ["overwrite", "append"]:
            raise Exception(self.job_name + ' : Values with Parquet targetPath should be in [overwrite,append]')
