# Copyright (c) 2025-2026 ADBC Drivers Contributors
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

import contextlib
import functools
import os
import re
import uuid
from pathlib import Path

from adbc_drivers_validation import model, quirks
from adbc_drivers_validation.tests import ingest


class Spark3ThriftQuirks(model.DriverQuirks):
    name = "spark3"
    driver = "adbc_driver_spark"
    driver_name = "ADBC Driver Foundry Driver for Apache Spark"
    vendor_name = "Apache Spark"
    vendor_version = re.compile(r"3\.5\.\d+.*\(HiveServer2\+binary\)")
    short_version = "3.5-thrift"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_set_current_catalog=True,
        connection_set_current_schema=True,
        get_objects=True,
        statement_bind=False,
        statement_bulk_ingest=True,
        statement_prepare=True,
        statement_rows_affected=True,
        current_catalog="spark_catalog",
        current_schema="default",
        secondary_catalog="hivealt",
        secondary_catalog_schema="default",
        secondary_schema="schematwo",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_URI"),
            "spark.ingest.s3.use_path_style": "true",
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": "s3://test/temporary",
        },
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            Path(__file__).parent.parent / "queries/base",
            Path(__file__).parent.parent / "queries/spark35",
        )

    def bind_parameter(self, index: int) -> str:
        return f"${index}"

    def quote_one_identifier(self, identifier: str) -> str:
        identifier = identifier.replace("`", "``")
        return f"`{identifier}`"

    def query_override(self, context: str, default: str) -> str:
        default = super().query_override(context, default)
        if context in (
            "TestConnection.test_get_table_schema_catalog",
            "TestConnection.test_get_table_schema_schema",
        ):
            return default.replace("VARCHAR", "STRING")
        return default

    def drop_table(
        self,
        *,
        table_name: str,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        if_exists: bool = True,
        temporary: bool = False,
    ) -> str:
        if temporary:
            if catalog_name or schema_name:
                raise ValueError("Cannot pass catalog/schema name for temporary table")
            table_name = self.qualify_temp_table(table_name)
            if if_exists:
                return f"DROP TABLE IF EXISTS {table_name}"
            return f"DROP TABLE {table_name}"

        return super().drop_table(
            table_name=table_name,
            schema_name=schema_name,
            catalog_name=catalog_name,
            if_exists=if_exists,
            temporary=temporary,
        )

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        msg = str(error)
        return (
            "TABLE_OR_VIEW_NOT_FOUND" in msg or "Failed to get table info" in msg
        ) and table_name in msg

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)


class Spark3LivyQuirks(Spark3ThriftQuirks):
    name = "spark3"
    vendor_version = re.compile(r"3\.5\.\d+.*\(Apache Livy\)")
    short_version = "3.5-livy"
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_LIVY_URI"),
            "spark.ingest.s3.use_path_style": "true",
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": "s3://test/temporary",
        },
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            Path(__file__).parent.parent / "queries/base",
            Path(__file__).parent.parent / "queries/spark35",
            Path(__file__).parent.parent / "queries/spark35-livy",
        )


class Spark4ThriftQuirks(Spark3ThriftQuirks):
    name = "spark4"
    vendor_version = re.compile(r"4\.0\.\d+.*\(HiveServer2\+binary\)")
    short_version = "4.0-thrift"

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            Path(__file__).parent.parent / "queries/base",
            Path(__file__).parent.parent / "queries/spark40",
            Path(__file__).parent.parent / "queries/spark40-thrift",
        )


class Spark4ThriftHttpQuirks(Spark4ThriftQuirks):
    vendor_version = re.compile(r"4\.0\.\d+.*\(HiveServer2\+HTTP\)")
    short_version = "4.0-thrifthttp"

    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_THRIFTHTTP_URI"),
            "spark.ingest.s3.use_path_style": "true",
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": "s3://test/temporary",
        },
    )


class Spark4ConnectQuirks(Spark4ThriftQuirks):
    vendor_version = re.compile(r"4\.0\.\d+.*\(Spark Connect\)")
    short_version = "4.0-connect"
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_CONNECT_URI"),
            "spark.ingest.s3.use_path_style": "true",
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": "s3://test/temporary",
        },
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            Path(__file__).parent.parent / "queries/base",
            Path(__file__).parent.parent / "queries/spark40",
            Path(__file__).parent.parent / "queries/spark40-connect",
        )


class Spark41ConnectQuirks(Spark4ConnectQuirks):
    vendor_version = re.compile(r"4\.1\.\d+.*\(Spark Connect\)")
    short_version = "4.1-connect"
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK41_CONNECT_URI"),
            "spark.ingest.s3.use_path_style": "true",
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": "s3://test/temporary",
        },
    )


class Spark41ConnectIcebergQuirks(Spark41ConnectQuirks):
    name = "spark4iceberg"
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK41_CONNECT_URI"),
            "spark.ingest.s3.use_path_style": "true",
            "adbc.connection.catalog": "iceberg",
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": "s3://test/temporary",
        },
    )

    features = Spark41ConnectQuirks.features.with_values(
        current_catalog="iceberg",
        secondary_catalog="icebergalt",
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            *super().queries_paths,
            Path(__file__).parent.parent / "queries/spark41-iceberg",
        )


class SparkEmr8ConnectQuirks(Spark4ConnectQuirks):
    short_version = "emr-8.0-connect"

    features = Spark4ConnectQuirks.features.with_values(
        secondary_catalog="ghatestcatalog",
        secondary_catalog_schema="myschema",
        secondary_schema="myschema",
    )

    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_CONNECT_URI"),
        },
        connection={},
        statement={
            "spark.ingest.staging_area_uri": model.FromEnv(
                "STAGING_S3_BUCKET", template="s3://{}/emr/staging"
            ),
        },
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            Path(__file__).parent.parent / "queries/base",
            Path(__file__).parent.parent / "queries/spark40",
            Path(__file__).parent.parent / "queries/spark40-connect",
            Path(__file__).parent.parent / "queries/emr-spark8",
        )

    def query_override(self, context: str, default: str) -> str:
        default = super().query_override(context, default)
        if context in (
            "TestConnection.test_get_table_schema_catalog",
            "TestConnection.test_get_table_schema_schema",
        ):
            location = model.FromEnv("STAGING_S3_BUCKET", template="s3://{}/emr/")
            default += " LOCATION '"
            default += location.get_or_raise()
            default += str(uuid.uuid4())
            default += "/'"
        return default

    def drop_table(
        self,
        *,
        table_name: str,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        if_exists: bool = True,
        temporary: bool = False,
    ) -> str:
        query = super().drop_table(
            table_name=table_name,
            schema_name=schema_name,
            catalog_name=catalog_name,
            if_exists=if_exists,
            temporary=temporary,
        )
        if table_name == "test_get_table_schema_catalog":
            # Iceberg catalog requires PURGE
            query += " PURGE"
        return query

    @contextlib.contextmanager
    def setup_statement(self, query, cursor):
        prefix = str(uuid.uuid4())
        location = model.FromEnv("STAGING_S3_BUCKET", template="s3://{}/emr/")
        with super().setup_statement(query, cursor):
            if isinstance(query, model.Query) and isinstance(
                query.query, model.IngestQuery
            ):
                name = ingest.make_table_name(prefix, query)
                location = location.get_or_raise() + name
                cursor.adbc_statement.set_options(
                    **{
                        "spark.ingest.location": location,
                    }
                )
                print(f"{query.name}: spark.ingest.location =", location)
            elif isinstance(query, str) and (
                query.startswith("TestIngest")
                or query == "TestConnection.get_objects_table"
            ):
                prefix = str(uuid.uuid4())
                name = ingest.make_table_name(prefix, query.split(".")[-1])
                location = location.get_or_raise() + name
                cursor.adbc_statement.set_options(
                    **{
                        "spark.ingest.location": location,
                    }
                )
                print(f"{query}: spark.ingest.location =", location)

            yield

    def split_statement(self, statement: str) -> list[str]:
        parts = super().split_statement(statement)
        bucket = os.environ.get("STAGING_S3_BUCKET")
        if bucket is None or not bucket:
            raise ValueError("STAGING_S3_BUCKET environment variable is not set")
        return [
            part.replace("{uuid}", str(uuid.uuid4())).replace("{bucket}", bucket)
            for part in parts
            if part
        ]


_VERSION_RE = re.compile(
    r"^(spark3|spark4|emr|spark4iceberg)(?:_|:)((?:emr-)?\d+\.\d+-(?:connect|livy|thrift|thrifthttp))$"
)


@functools.cache
def get_quirks(combined_version: str) -> model.DriverQuirks:
    m = _VERSION_RE.match(combined_version)
    if m is None:
        raise ValueError(f"invalid version format: {combined_version}")

    vendor = m.group(1)
    version = m.group(2)
    if vendor in ("spark3",):
        if version == "3.5-thrift":
            return Spark3ThriftQuirks()
        elif version == "3.5-livy":
            return Spark3LivyQuirks()
    elif vendor in ("spark4",):
        if version == "4.0-thrift":
            return Spark4ThriftQuirks()
        elif version == "4.0-thrifthttp":
            return Spark4ThriftHttpQuirks()
        elif version == "4.0-connect":
            return Spark4ConnectQuirks()
        elif version == "4.1-connect":
            return Spark41ConnectQuirks()
        elif version in ("emr-8.0-connect"):
            return SparkEmr8ConnectQuirks()
    elif vendor in ("spark4iceberg",):
        if version == "4.1-connect":
            return Spark41ConnectIcebergQuirks()
    elif vendor in ("emr",):
        if version in ("8.0-connect"):
            return SparkEmr8ConnectQuirks()
    raise ValueError(f"unsupported Spark {vendor} {version}")
