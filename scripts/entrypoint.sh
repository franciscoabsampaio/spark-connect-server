#!/bin/bash
set -euo pipefail

# Setup SSL if enabled
source ${SPARK_HOME}/setup_ssl.sh

# Set catalog JARs based on the catalog type
if [ ${CATALOG} == delta ]; then
  CATALOG_JARS=${SPARK_HOME}/jars/delta-spark_${SCALA_VERSION}-${CATALOG_VERSION}.jar,${SPARK_HOME}/jars/delta-storage-${CATALOG_VERSION}.jar
else
  SPARK_VERSION_MAJOR=$(echo "${SPARK_VERSION}" | cut -d'.' -f1,2)
  CATALOG_JARS=${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}-${CATALOG_VERSION}.jar
fi
# Set Spark Connect JARs
SPARK_CONNECT_JAR=${SPARK_HOME}/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar

# Start Spark Connect Server
/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --conf spark.connect.grpc.binding=0.0.0.0:15002 \
  $SSL_ARGS \
  --jars ${SPARK_CONNECT_JAR},${CATALOG_JARS} \
  --name spark-connect-server