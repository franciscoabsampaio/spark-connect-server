ARG CATALOG_VERSION
ARG JAVA_VERSION
ARG SCALA_VERSION
ARG SPARK_VERSION

FROM spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java${JAVA_VERSION}-ubuntu AS base

ARG CATALOG_VERSION
ARG JAVA_VERSION
ARG SCALA_VERSION
ARG SPARK_VERSION

ENV CATALOG_VERSION=${CATALOG_VERSION}
ENV JAVA_VERSION=${JAVA_VERSION}
ENV SCALA_VERSION=${SCALA_VERSION}
ENV SPARK_VERSION=${SPARK_VERSION}
ENV USE_SSL=false

USER root

# Install wget and remove apt cache
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Download Spark Connect JAR (common to all catalogs)
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    -P ${SPARK_HOME}/jars/

RUN mkdir -p ${SPARK_HOME}/conf && \
    mkdir -p /opt/ssl && \
    mkdir -p /tmp/warehouse && \
    chown -R spark:spark /opt/ssl /tmp/warehouse

# Copy scripts and make them executable
COPY scripts/setup_ssl.sh ${SPARK_HOME}/setup_ssl.sh
COPY scripts/entrypoint.sh ${SPARK_HOME}/entrypoint.sh
RUN chmod +x ${SPARK_HOME}/setup_ssl.sh ${SPARK_HOME}/entrypoint.sh

USER spark
WORKDIR ${SPARK_HOME}

EXPOSE 15002/tcp
EXPOSE 4040/tcp

# Reports the container healthy once Spark Connect accepts connections, so
# consumers can wait on `docker inspect` health, compose's `service_healthy` or
# a readiness probe instead of scraping the log. Probed from inside the
# container: a published port is served by Docker's proxy, which accepts
# connections before the server itself listens. Failures during `start-period`
# delay `healthy` rather than marking the container unhealthy, which covers
# slow JVM startup.
HEALTHCHECK --interval=5s --timeout=3s --start-period=180s --retries=60 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/15002'

ENTRYPOINT ["sh", "-c", "$SPARK_HOME/entrypoint.sh"]