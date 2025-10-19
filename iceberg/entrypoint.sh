#!/bin/bash
# Start Spark Connect Server
/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --jars ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}-${CATALOG_VERSION}.jar,${SPARK_HOME}/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
  --name spark-connect-server