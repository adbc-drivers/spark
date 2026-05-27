# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG SPARK_VERSION=4.0.0
ARG HIVE_VERSION=4.0.1
FROM apache/hive:${HIVE_VERSION} AS hive-jars

FROM apache/spark:${SPARK_VERSION} AS base

ARG SPARK_VERSION_NO_PATCH=4.0
ARG ICEBERG_VERSION=1.11.0
ARG SCALA_VERSION=2.13

USER root

RUN apt-get update && apt-get install -y \
    curl wget unzip krb5-user \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark-data/warehouse && chmod 777 /opt/spark-data/warehouse

ENV SPARK_HOME=/opt/spark

WORKDIR /tmp

# Copy Hive 4 metastore client JARs to a separate directory for isolated
# classloading. Spark 4 ships hive-metastore-2.3.10 which uses old Thrift
# method names incompatible with Hive 4 standalone metastore. Using
# spark.sql.hive.metastore.jars=path loads these in an isolated classloader
# so they don't conflict with Spark's own Hive service classes.
RUN mkdir -p /opt/hive4-metastore-jars
COPY --from=hive-jars /opt/hive/lib/ /opt/hive4-metastore-jars/

# Download Iceberg runtime (Spark 4.0 + Scala 2.13)
ARG ICEBERG_NAME="iceberg-spark-runtime-${SPARK_VERSION_NO_PATCH}_${SCALA_VERSION}"
ARG ICEBERG_JAR="${ICEBERG_NAME}-${ICEBERG_VERSION}.jar"
RUN wget -q https://repo1.maven.org/maven2/org/apache/iceberg/${ICEBERG_NAME}/${ICEBERG_VERSION}/${ICEBERG_JAR} -P ${SPARK_HOME}/jars/

# Download Hadoop AWS and AWS SDK for S3 (MinIO) support
# Version must match the Hadoop bundled with Spark (4.0.x ships Hadoop 3.4.1)
ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_V2_VERSION=2.24.6
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/ \
    && wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar -P ${SPARK_HOME}/jars/

COPY kerberos/conf/krb5.conf /etc/krb5.conf
ENV KRB5CCNAME="/tmp/krb5cc_spark"


# ------
# THRIFT
# ------
FROM base AS thrift

COPY --from=base ${SPARK_HOME} ${SPARK_HOME}
COPY hive4/conf/ ${SPARK_HOME}/conf/
COPY spark4/conf/ ${SPARK_HOME}/conf/
COPY spark4/entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

WORKDIR ${SPARK_HOME}

ENV SPARK_SERVER_TYPE=thrift

CMD ["/opt/entrypoint.sh"]



# -------
# CONNECT
# -------
FROM base AS connect

COPY --from=base ${SPARK_HOME} ${SPARK_HOME}
COPY hive4/conf/ ${SPARK_HOME}/conf/
COPY spark4/conf/ ${SPARK_HOME}/conf/
COPY spark4/entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

WORKDIR ${SPARK_HOME}

ENV SPARK_SERVER_TYPE=connect

CMD ["/opt/entrypoint.sh"]
