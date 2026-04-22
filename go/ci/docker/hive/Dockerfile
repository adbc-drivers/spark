ARG HIVE_VERSION=3.1.3
# ARG HIVE_VERSION=standalone-metastore-4.2.0
FROM apache/hive:${HIVE_VERSION} AS hive

ARG POSTGRES_DRIVER_VERSION=42.7.9

ENV HIVE_HOME=/opt/hive

ARG POSTGRES_NAME=postgresql-${POSTGRES_DRIVER_VERSION}

USER root
RUN export DEBIAN_FRONTEND=noninteractive \
        && apt-get update \
        && apt-get install -y --no-install-recommends net-tools krb5-user \
        && rm /opt/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar

# NOTE: If Hive >= 4, there, the prebuilt docker images contain wget.
# Otherwise you need the local Postgres driver JAR in your workspace directory.

# RUN wget -q https://jdbc.postgresql.org/download/${POSTGRES_NAME}.jar -P ${HIVE_HOME}/lib/
COPY ./hive/postgresql-${POSTGRES_DRIVER_VERSION}.jar ${HIVE_HOME}/lib/
COPY ./hive/conf/ ${HIVE_HOME}/conf/
COPY ./kerberos/conf/krb5.conf /etc/krb5.conf

# Add entrypoint for schema initialization
COPY ./hive/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["sh", "/entrypoint.sh"]
