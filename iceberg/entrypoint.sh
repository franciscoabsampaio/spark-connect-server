#!/bin/bash
SPARK_VERSION_MAJOR=$(echo "${SPARK_VERSION}" | cut -d'.' -f1,2)

# Start Spark Connect Server
/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --jars ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}-${CATALOG_VERSION}.jar,${SPARK_HOME}/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
  --name spark-connect-server