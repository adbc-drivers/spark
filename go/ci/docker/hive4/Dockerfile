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

ARG HIVE_VERSION=4.0.1
FROM apache/hive:${HIVE_VERSION} AS hive

ARG POSTGRES_DRIVER_VERSION=42.7.9

ENV HIVE_HOME=/opt/hive

ARG POSTGRES_NAME=postgresql-${POSTGRES_DRIVER_VERSION}

USER root
RUN export DEBIAN_FRONTEND=noninteractive \
        && apt-get update \
        && apt-get install -y --no-install-recommends net-tools krb5-user wget postgresql-client \
        && rm -rf /var/lib/apt/lists/*

RUN wget -q https://jdbc.postgresql.org/download/${POSTGRES_NAME}.jar -P ${HIVE_HOME}/lib/
COPY ./hive4/conf/ ${HIVE_HOME}/conf/
COPY ./kerberos/conf/krb5.conf /etc/krb5.conf

COPY ./hive4/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["sh", "/entrypoint.sh"]
