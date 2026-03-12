ARG HIVE_VERSION=3.1.3
FROM apache/hive:${HIVE_VERSION} AS hive

ARG POSTGRES_DRIVER_VERSION=42.7.9

ENV HIVE_HOME=/opt/hive

ARG POSTGRES_NAME=postgresql-${POSTGRES_DRIVER_VERSION}

# NOTE: If Hive >= 4, there, the prebuilt docker images contain wget.
# Otherwise you need the local Postgres driver JAR in your workspace directory.

# RUN wget -q https://jdbc.postgresql.org/download/${POSTGRES_NAME}.jar -P ${HIVE_HOME}/lib/
COPY ./postgresql-${POSTGRES_DRIVER_VERSION}.jar ${HIVE_HOME}/lib/
