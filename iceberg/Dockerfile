ARG CATALOG_VERSION
ARG JAVA_VERSION
ARG SCALA_VERSION
ARG SPARK_VERSION

# Base image
FROM spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java${JAVA_VERSION}-ubuntu

ARG CATALOG_VERSION
ARG JAVA_VERSION
ARG SCALA_VERSION
ARG SPARK_VERSION

# Switch to root to install dependencies
USER root

# Expose build arguments as environment variables to the entrypoint script
ENV CATALOG_VERSION=${CATALOG_VERSION}
ENV JAVA_VERSION=${JAVA_VERSION}
ENV SCALA_VERSION=${SCALA_VERSION}
ENV SPARK_VERSION=${SPARK_VERSION}

# Derive SPARK_VERSION_MAJOR in a RUN, then persist it
RUN SPARK_VERSION_MAJOR=$(echo "$SPARK_VERSION" | cut -d'.' -f1,2) && \
    echo "SPARK_VERSION_MAJOR=$SPARK_VERSION_MAJOR" >> /etc/environment
ENV SPARK_VERSION_MAJOR=${SPARK_VERSION_MAJOR}

# Download the Iceberg and Spark Connect JARs into the Spark jars directory
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/* && \
    # Spark Connect
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
      -P ${SPARK_HOME}/jars/ && \
    # Iceberg core libraries
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}/${CATALOG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}-${CATALOG_VERSION}.jar \
      -P ${SPARK_HOME}/jars/

# Ensure conf dir exists
RUN mkdir -p ${SPARK_HOME}/conf

# Write default configs for Iceberg
RUN printf "%s\n" \
      "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
      "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
      "spark.sql.catalog.local.type=hadoop" \
      "spark.sql.catalog.local.warehouse=/tmp/warehouse" \
      "spark.sql.defaultCatalog=local" \
      > ${SPARK_HOME}/conf/spark-defaults.conf && \
    mkdir -p /tmp/warehouse && chown -R spark:spark /tmp/warehouse

# Revert to the spark user
USER spark

WORKDIR /opt/spark

# Copy entrypoint script and execute it
COPY entrypoint.sh /opt/entrypoint.sh
USER root
RUN chmod +x /opt/entrypoint.sh
USER spark

# Spark Connect Server
EXPOSE 15002/tcp
# Spark UI
EXPOSE 4040/tcp

ENTRYPOINT ["/opt/entrypoint.sh"]
