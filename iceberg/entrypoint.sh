#!/bin/bash
# Start Spark Connect Server
/opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.connect.service.SparkConnectServer \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar,/opt/spark/jars/spark-connect_2.12-3.5.7.jar \
  --name spark-connect-server