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

ARG SPARK_VERSION=3.5.7
FROM apache/spark:${SPARK_VERSION} AS base

ARG SPARK_VERSION=3.5
ARG ICEBERG_VERSION=1.7.0
ARG SCALA_VERSION=2.12

USER root

RUN apt-get update && apt-get install -y \
    curl wget unzip krb5-user \
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

COPY spark/conf/ ${LIVY_HOME}/conf/

EXPOSE 8998
WORKDIR ${LIVY_HOME}

CMD ["/bin/bash", "-c", "$LIVY_HOME/bin/livy-server"]



# ------
# THRIFT
# ------

FROM base AS thrift

COPY --from=base ${SPARK_HOME} ${SPARK_HOME}
COPY hive/conf/ ${SPARK_HOME}/conf/
COPY kerberos/conf/krb5.conf /etc/krb5.conf
COPY spark/conf/ ${SPARK_HOME}/conf/
COPY spark/entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

WORKDIR ${SPARK_HOME}

ENV KRB5CCNAME="/tmp/krb5cc_spark"

CMD ["/opt/entrypoint.sh"]
