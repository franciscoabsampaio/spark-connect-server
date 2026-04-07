FROM base AS delta

USER root

# Download the Delta Lake JARs directly into Spark's jars directory
# This avoids the need for the --packages argument at runtime.
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${CATALOG_VERSION}/delta-spark_${SCALA_VERSION}-${CATALOG_VERSION}.jar \
    -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/${CATALOG_VERSION}/delta-storage-${CATALOG_VERSION}.jar \
    -P ${SPARK_HOME}/jars/

# Write default configs for Delta
RUN printf "%s\n" \
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        "spark.sql.warehouse.dir=/tmp/warehouse" \
    > ${SPARK_HOME}/conf/spark-defaults.conf

USER spark

# Copy entrypoint script and make it executable
COPY entrypoint.sh /opt/entrypoint.sh
USER root
RUN chmod +x /opt/entrypoint.sh
USER spark

ENTRYPOINT ["/opt/entrypoint.sh"]
