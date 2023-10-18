#!/bin/bash
/opt/spark/bin/spark-submit \
--master local[*] \
--deploy-mode client \
--num-executors 4 \
--driver-memory 1G \
--executor-memory 1G \
--executor-cores 2 \
--jars path/SparkDataSync/lib/postgresql-42.6.0.jar  \
--driver-class-path path/SparkDataSync/lib/postgresql-42.6.0.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files ../config/log4j.properties \
../main.py --job_file $1
