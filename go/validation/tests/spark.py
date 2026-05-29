# Copyright (c) 2025 ADBC Drivers Contributors
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

import functools
import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class Spark3ThriftQuirks(model.DriverQuirks):
    name = "spark3"
    driver = "adbc_driver_spark"
    driver_name = "ADBC Driver Foundry Driver for Apache Spark"
    vendor_name = "Apache Spark"
    vendor_version = re.compile(r"3\.5\.\d+.*\(HiveServer2\)")
    short_version = "3.5-thrift"
    features = model.DriverFeatures(
        get_objects=True,
        statement_bind=False,
        statement_bulk_ingest=True,
        statement_prepare=True,
        statement_rows_affected=True,
        current_catalog="spark_catalog",
        current_schema="default",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_URI"),
            "username": "spark",
            "password": "spark",
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

    def drop_table(
        self,
        *,
        table_name: str,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        if_exists: bool = True,
        temporary: bool = False,
    ) -> None:
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
        return "TABLE_OR_VIEW_NOT_FOUND" in str(error) and table_name in str(error)

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)


class Spark3LivyQuirks(Spark3ThriftQuirks):
    name = "spark3"
    vendor_version = re.compile(r"3\.5\.\d+.*\(Apache Livy\)")
    short_version = "3.5-livy"
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_LIVY_URI"),
            "username": "spark",
            "password": "spark",
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
    vendor_version = re.compile(r"4\.0\.\d+.*\(HiveServer2\)")
    short_version = "4.0-thrift"

    @property
    def queries_paths(self) -> tuple[Path]:
        return (
            Path(__file__).parent.parent / "queries/base",
            Path(__file__).parent.parent / "queries/spark40",
        )


class Spark4ConnectQuirks(Spark4ThriftQuirks):
    vendor_version = re.compile(r"4\.0\.\d+.*\(Spark Connect\)")
    short_version = "4.0-connect"
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SPARK_CONNECT_URI"),
            "username": "spark",
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


_VERSION_RE = re.compile(r"^(spark3|spark4)(?:_|:)(\d+\.\d+-(connect|livy|thrift))$")


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
        elif version == "4.0-connect":
            return Spark4ConnectQuirks()
    raise ValueError(f"unsupported Spark {vendor} {version}")
