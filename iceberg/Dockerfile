FROM base AS iceberg

USER root

# Download the Iceberg jar into the Spark jars directory
RUN SPARK_VERSION_MAJOR=$(echo "${SPARK_VERSION}" | cut -d'.' -f1,2) && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}/${CATALOG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION_MAJOR}_${SCALA_VERSION}-${CATALOG_VERSION}.jar \
      -P ${SPARK_HOME}/jars/
      
# Write default configs for Iceberg
RUN printf "%s\n" \
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
        "spark.sql.catalog.local.type=hadoop" \
        "spark.sql.catalog.local.warehouse=/tmp/warehouse" \
        "spark.sql.defaultCatalog=local" \
    > ${SPARK_HOME}/conf/spark-defaults.conf

USER spark

# Copy entrypoint script and make it executable
COPY entrypoint.sh /opt/entrypoint.sh
USER root
RUN chmod +x /opt/entrypoint.sh
USER spark

ENTRYPOINT ["/opt/entrypoint.sh"]