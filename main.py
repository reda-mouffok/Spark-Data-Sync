import argparse
from src.data_tools import data_tools
from src.data_tools.spark_builder import create_spark_session
from src.data_tools.spark_logger import Log4j
from src.models.data_job import DataJob
from src.models.jdbc_connection import JDBCConnection


# TODO add XML,JSON,HIVE,KAFKA data source
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spark_config',
                        help='Configuration file of spark application',
                        default="../config/spark_config.yaml",
                        )
    parser.add_argument('--jdbc_file',
                        help='JDBC connection file',
                        default="../config/jdbc_connection.yaml",
                        )
    parser.add_argument('--job_files',
                        help='file containing the configuration of jobs',
                        required=True
                        )
    args = parser.parse_args()

    # Create the SparkSession
    spark = create_spark_session(args.spark_config)
    logger = Log4j(spark)
    logger.info("Spark Export Begin")

    # Create the JDBC connection
    jdbc_connection = JDBCConnection(args.jdbc_file)

    # Read jobs to be executed
    job_list = []
    jobs = data_tools.read_yaml(args.job_files)

    for job_name, job_attribute in jobs.items():
        job_list.append(DataJob(spark, jdbc_connection, job_name, job_attribute))

    for data_job in job_list:
        logger.info("----------< Execution of " + data_job.job_name + " Begin >---------")
        data_job.execute_job()
        logger.info("----------< Execution of " + data_job.job_name + " Finish >---------")

    # Stop the Spark Session
    logger.info("Spark export finish")
    spark.stop()


if __name__ == '__main__':
    main()
