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
# ARG HIVE_VERSION=standalone-metastore-4.2.0
FROM apache/hive:${HIVE_VERSION} AS hive

ARG POSTGRES_DRIVER_VERSION=42.7.9

ENV HIVE_HOME=/opt/hive

ARG POSTGRES_NAME=postgresql-${POSTGRES_DRIVER_VERSION}

USER root
RUN export DEBIAN_FRONTEND=noninteractive \
        && apt-get update \
        && apt-get install -y --no-install-recommends net-tools krb5-user wget \
        && rm /opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar

# NOTE: If Hive >= 4, there, the prebuilt docker images contain wget.
# Otherwise you need the local Postgres driver JAR in your workspace directory.

RUN wget -q https://jdbc.postgresql.org/download/${POSTGRES_NAME}.jar -P ${HIVE_HOME}/lib/
COPY ./hive/conf/ ${HIVE_HOME}/conf/
COPY ./kerberos/conf/krb5.conf /etc/krb5.conf

# Add entrypoint for schema initialization
COPY ./hive/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["sh", "/entrypoint.sh"]
