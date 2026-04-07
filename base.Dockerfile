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

ENTRYPOINT ["sh", "-c", "$SPARK_HOME/entrypoint.sh"]