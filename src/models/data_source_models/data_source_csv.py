from pyspark.sql.utils import AnalysisException

from src.data_tools.data_tools import read_csv
from src.models.data_source_models.data_source import DataSource


class DataSourceCSV(DataSource):

    def read_dataframe(self):
        try:
            return read_csv(self.spark, self.filePath)
        except AnalysisException as fileException:
            if self.read_tolerance:
                print(self.jobName + " => File not found in path  " + self.filePath)
                return None
            else:
                raise Exception(self.jobName + " => File not found in path  " + self.filePath)

    def check_yaml_schema(self):
        if not self.filePath:
            raise Exception(self.jobName + " : CSV sourceType should contain filePath attribute")
