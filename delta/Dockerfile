ARG SPARK_VERSION
ARG SCALA_VERSION
ARG CATALOG_VERSION

# Base image
FROM apache/spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java17-python3-r-ubuntu

# Switch to root to install dependencies
USER root

# Download the Delta Lake JARs directly into Spark's jars directory
# This avoids the need for the --packages argument at runtime.
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/* && \
    wget https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${CATALOG_VERSION}/delta-spark_${SCALA_VERSION}-${CATALOG_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/${CATALOG_VERSION}/delta-storage-${CATALOG_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar -P ${SPARK_HOME}/jars/

# Ensure conf dir exists
RUN mkdir -p ${SPARK_HOME}/conf

# Write default configs for Delta
RUN printf "%s\n" \
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    > ${SPARK_HOME}/conf/spark-defaults.conf

# Revert to the spark user
USER spark

# Set working directory to /opt/spark
WORKDIR /opt/spark

# Copy entrypoint script and execute it
COPY entrypoint.sh /opt/entrypoint.sh
USER root
RUN chmod +x /opt/entrypoint.sh
USER spark

# SparkUI
EXPOSE 15002/tcp
# Spark Connect Server
EXPOSE 4040/tcp

ENTRYPOINT ["/opt/entrypoint.sh"]
