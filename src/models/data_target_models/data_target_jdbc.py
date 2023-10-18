from functools import reduce

from pyspark import StorageLevel

from src.data_tools.data_tools import union_by_adding_column
from src.models.data_target_models.data_target import DataTarget


class DataTargetJDBC(DataTarget):

    def check_yaml_schema(self):
        if not self.table_name:
            raise Exception(
                self.job_name + " : " + self.target_type + " targetType should contain tableName attribute"
            )
        if self.job_mode not in ["overwrite", "append", "scd1"]:
            raise Exception(self.job_name + ' : Values with CSV targetPath should be in [overwrite,append,scd1]')
        if not self.source_key and self.job_mode == "scd1":
            raise Exception(self.job_name + ' : sourceKey should be provided for SCD1 Export')
        if not self.target_key and self.job_mode == "scd1":
            raise Exception(self.job_name + ' : targetKey should be provided for SCD1 Export')

    def write_dataframe(self, df):
        if self.job_mode in ["overwrite", "append"]:
            self.write_dataframe_with_mode(df)
        elif self.job_mode == "scd1":
            self.write_dataframe_with_scd1(df)
        else:
            pass

    def write_dataframe_with_mode(self, df):
        df.write \
            .format("jdbc") \
            .mode(self.job_mode) \
            .option("url", self.jdbc_connection.get_url(self.target_type)) \
            .option("dbtable", self.table_name) \
            .option("user", self.jdbc_connection.get_user(self.target_type)) \
            .option("password", self.jdbc_connection.get_password(self.target_type)) \
            .option("numPartitions", "10") \
            .option("driver", self.jdbc_connection.get_driver_type(self.target_type)) \
            .save()

    def write_dataframe_with_scd1(self, new_df):
        old_df = self.read_dataframe_from_jdbc()
        df_diff = self.get_scd1_from_dataframe(new_df, old_df)
        self.write_dataframe_overwrite_mode(df_diff)

    def write_dataframe_overwrite_mode(self, df):
        df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", self.jdbc_connection.get_url(self.target_type)) \
            .option("dbtable", self.table_name) \
            .option("user", self.jdbc_connection.get_user(self.target_type)) \
            .option("password", self.jdbc_connection.get_password(self.target_type)) \
            .option("numPartitions", "10") \
            .option("driver", self.jdbc_connection.get_driver_type(self.target_type)) \
            .save()

    def get_scd1_from_dataframe(self, new_df, old_df):
        if len(self.target_key) == len(self.source_key) and len(self.source_key) > 0:
            join_condition = reduce(lambda a, b: a == b,
                                    map(lambda new_key, old_key:
                                        new_df[new_key] == old_df[old_key],
                                        self.source_key,
                                        self.target_key
                                        )
                                    )
            insert_df = new_df.join(old_df, join_condition, "leftanti")
            update_df = new_df.alias("new") \
                .join(old_df, join_condition, "inner") \
                .select(*map(lambda x: "new." + x, new_df.columns))
            unchanged_df = old_df \
                .join(new_df, join_condition, "leftanti") \
                .persist(StorageLevel.MEMORY_AND_DISK)
            df = union_by_adding_column(
                union_by_adding_column(update_df, unchanged_df), insert_df
            )
            return df

    def read_dataframe_from_jdbc(self):
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_connection.get_url(self.target_type)) \
            .option("dbtable", self.table_name) \
            .option("user", self.jdbc_connection.get_user(self.target_type)) \
            .option("password", self.jdbc_connection.get_password(self.target_type)) \
            .option("numPartitions", "10") \
            .option("driver", self.jdbc_connection.get_driver_type(self.target_type)) \
            .load()
