#!/bin/bash
# Start Spark Connect Server
/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --conf spark.connect.grpc.binding=0.0.0.0:15002 \
  --jars ${SPARK_HOME}/jars/delta-spark_${SCALA_VERSION}-${CATALOG_VERSION}.jar,${SPARK_HOME}/jars/delta-storage-${CATALOG_VERSION}.jar,${SPARK_HOME}/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
  --name spark-connect-server