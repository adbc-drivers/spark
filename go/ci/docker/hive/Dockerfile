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

ARG HIVE_VERSION=3.1.3
ARG HADOOP_VERSION=3.1.0
ARG TEZ_VERSION=0.9.1

# Adapted from Apache Hive's packaging/src/docker/Dockerfile at
# 881d38fb4d7af3ed4fa55b71598b5057080f8829.
FROM eclipse-temurin:8-jre-ubi10-minimal AS distributions

ARG HIVE_VERSION
ARG HADOOP_VERSION
ARG TEZ_VERSION

RUN set -eux; \
    wget -q https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz; \
    wget -q https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz -O /tmp/hive.tar.gz; \
    wget -q https://archive.apache.org/dist/tez/${TEZ_VERSION}/apache-tez-${TEZ_VERSION}-bin.tar.gz -O /tmp/tez.tar.gz; \
    mkdir -p /opt/hadoop /opt/hive /opt/tez; \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/hadoop --strip-components=1; \
    tar -xzf /tmp/hive.tar.gz -C /opt/hive --strip-components=1; \
    tar -xzf /tmp/tez.tar.gz -C /opt/tez --strip-components=1; \
    rm -rf /opt/hadoop/share/doc /opt/hive/jdbc /opt/tez/share; \
    rm /tmp/hadoop.tar.gz /tmp/hive.tar.gz /tmp/tez.tar.gz; \
    rm /opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar

FROM eclipse-temurin:8-jre-ubi10-minimal AS hive

ARG HIVE_VERSION
ARG POSTGRES_DRIVER_VERSION=42.7.9

ENV HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    TEZ_HOME=/opt/tez \
    HIVE_VER=${HIVE_VERSION}
ENV PATH=${HIVE_HOME}/bin:${HADOOP_HOME}/bin:${PATH}

ARG POSTGRES_NAME=postgresql-${POSTGRES_DRIVER_VERSION}

USER root
RUN microdnf install -y procps-ng net-tools krb5-workstation postgresql \
        && microdnf clean all

COPY --from=distributions /opt/hadoop /opt/hadoop
COPY --from=distributions /opt/hive /opt/hive
COPY --from=distributions /opt/tez /opt/tez

RUN wget -q https://jdbc.postgresql.org/download/${POSTGRES_NAME}.jar -P ${HIVE_HOME}/lib/ \
        && mkdir -p ${HIVE_HOME}/data/warehouse
COPY ./hive/conf/ ${HIVE_HOME}/conf/
COPY ./kerberos/conf/krb5.conf /etc/krb5.conf

# Add entrypoint for schema initialization
COPY ./hive/entrypoint.sh /entrypoint.sh

WORKDIR /opt/hive
EXPOSE 10000 10002 9083
ENTRYPOINT ["sh", "/entrypoint.sh"]
