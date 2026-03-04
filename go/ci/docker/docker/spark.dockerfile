ARG SPARK_VERSION=3.5.7
FROM apache/spark:${SPARK_VERSION} AS base

ARG SPARK_VERSION=3.5
ARG ICEBERG_VERSION=1.7.0
ARG SCALA_VERSION=2.12

USER root

RUN apt-get update && apt-get install -y \
    curl wget unzip \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark-data/warehouse && chmod 777 /opt/spark-data/warehouse

ENV SPARK_HOME=/opt/spark

WORKDIR /tmp

# Download Iceberg runtime (Spark 3.5 + Scala 2.12)
ARG ICEBERG_NAME="iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}"
ARG ICEBERG_JAR="${ICEBERG_NAME}-${ICEBERG_VERSION}.jar"
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/${ICEBERG_NAME}/${ICEBERG_VERSION}/${ICEBERG_JAR} -P ${SPARK_HOME}/jars/



# ----
# LIVY
# ----
ARG LIVY_VERSION=0.8.0-incubating

FROM base AS livy

ENV LIVY_HOME=/opt/livy

COPY --from=base ${SPARK_HOME}/jars/${ICEBERG_JAR} ${LIVY_HOME}/jars/

ARG LIVY_NAME=apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin
RUN wget https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/${LIVY_NAME}.zip \
    && unzip ${LIVY_NAME}.zip \
    && cp -r ${LIVY_NAME}/* ${LIVY_HOME}/ \
    && rm -r ${LIVY_NAME}.zip ${LIVY_NAME}

RUN mkdir -p ${LIVY_HOME}/logs ${LIVY_HOME}/conf

COPY conf/livy.conf ${LIVY_HOME}/conf/livy.conf
COPY conf/spark-defaults.conf ${LIVY_HOME}/conf/spark-defaults.conf
COPY conf/log4j.properties ${LIVY_HOME}/conf/log4j.properties

EXPOSE 8998
WORKDIR ${LIVY_HOME}

CMD ["/bin/bash", "-c", "$LIVY_HOME/bin/livy-server"]



# ------
# THRIFT
# ------

FROM base AS thrift

COPY --from=base ${SPARK_HOME} ${SPARK_HOME}

WORKDIR ${SPARK_HOME}

CMD ["/bin/bash", "-c", "$SPARK_HOME/sbin/start-thriftserver.sh; tail -f $SPARK_HOME/logs/*thriftserver*.out"]
