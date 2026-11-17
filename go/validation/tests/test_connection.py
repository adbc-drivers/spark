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

import uuid

import adbc_driver_manager
import adbc_drivers_validation.tests.connection
import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = [spark.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


class TestConnection(adbc_drivers_validation.tests.connection.TestConnection):
    def test_get_objects_schema_name_is_not_a_pattern(self, driver, conn) -> None:
        if driver.short_version.startswith("emr-"):
            pytest.skip("EMR tables need an S3 location")
        # Spark's Thrift server reads the schema name in GetTables and
        # GetColumns as a pattern, in which '_' matches any character.
        suffix = uuid.uuid4().hex[:8]
        catalog = driver.features.current_catalog
        schema, other = f"pa_{suffix}", f"pax{suffix}"
        tables = [
            (schema, f"shared_{suffix}", "a"),
            (other, f"shared_{suffix}", "b"),
            (other, f"other_{suffix}", "c"),
        ]

        def qualified(*names):
            return ".".join(driver.quote_one_identifier(n) for n in (catalog, *names))

        with conn.cursor() as cursor:
            for s in (schema, other):
                cursor.execute(f"CREATE SCHEMA {qualified(s)}")
            for s, table, column in tables:
                cursor.execute(f"CREATE TABLE {qualified(s, table)} ({column} INT)")
        try:
            objects = conn.adbc_get_objects(
                depth="columns", catalog_filter=catalog, db_schema_filter=schema
            )
            listed = sorted(
                (t["table_name"], [c["column_name"] for c in t["table_columns"]])
                for obj in objects.read_all().to_pylist()
                for s in obj["catalog_db_schemas"]
                if s["db_schema_name"] == schema
                for t in s["db_schema_tables"]
                if t["table_name"].endswith(suffix)
            )
            assert listed == [(f"shared_{suffix}", ["a"])]
        finally:
            with conn.cursor() as cursor:
                for s, table, _ in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {qualified(s, table)}")
                for s in (schema, other):
                    cursor.execute(f"DROP SCHEMA IF EXISTS {qualified(s)}")

    def test_get_objects_temporary_view(self, driver, conn) -> None:
        # SHOW TABLES lists temporary views in every schema of the session
        # catalog, but DESCRIBE TABLE can't find them there. Other catalogs,
        # such as Iceberg, don't list them, so look in spark_catalog.default.
        view = f"temp_view_{uuid.uuid4().hex[:8]}"
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE TEMPORARY VIEW {driver.quote_one_identifier(view)} AS SELECT 1 AS id"
            )
        try:
            objects = conn.adbc_get_objects(
                depth="all",
                catalog_filter="spark_catalog",
                db_schema_filter="default",
                table_name_filter=view,
            )
            tables = [
                t["table_name"]
                for obj in objects.read_all().to_pylist()
                for s in obj["catalog_db_schemas"]
                for t in s["db_schema_tables"]
            ]
            assert tables == [view]
        finally:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DROP VIEW IF EXISTS {driver.quote_one_identifier(view)}"
                )

    def test_get_objects_table_types(self, driver, conn) -> None:
        if driver.short_version.endswith(("-thrift", "-thrifthttp")):
            # Spark's Thrift GetTables reports temporary views as VIEW, with its
            # own types. This test covers SHOW TABLE EXTENDED.
            pytest.skip("Thrift lists tables with GetTables")
        if driver.short_version.startswith("emr-"):
            pytest.skip("EMR tables need an S3 location")
        suffix = uuid.uuid4().hex[:8]
        catalog, schema = "spark_catalog", "default"
        managed = f"types_managed_{suffix}"
        external = f"types_external_{suffix}"
        view = f"types_view_{suffix}"
        temp_view = f"types_temp_{suffix}"

        def qualified(name):
            return ".".join(
                driver.quote_one_identifier(n) for n in (catalog, schema, name)
            )

        with conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE {qualified(managed)} (id INT)")
            cursor.execute(f"CREATE VIEW {qualified(view)} AS SELECT 1 AS id")
            cursor.execute(
                f"CREATE TEMPORARY VIEW {driver.quote_one_identifier(temp_view)} AS SELECT 1 AS id"
            )
            # An external table needs a location the server can use. Skip it
            # where the server refuses to create one.
            try:
                cursor.execute(
                    f"CREATE TABLE {qualified(external)} (id INT) USING parquet "
                    f"LOCATION '/tmp/adbc_types_{suffix}'"
                )
                has_external = True
            except adbc_driver_manager.dbapi.Error:
                has_external = False
        expected = {
            managed: "MANAGED",
            view: "VIEW",
            temp_view: "VIEW",
        }
        if has_external:
            expected[external] = "EXTERNAL"
        try:

            def listed(types_filter=None, depth="tables"):
                objects = conn.adbc_get_objects(
                    depth=depth,
                    catalog_filter=catalog,
                    db_schema_filter=schema,
                    table_name_filter=f"%{suffix}",
                    table_types_filter=types_filter,
                )
                return sorted(
                    (
                        t["table_name"],
                        t["table_type"],
                        [c["column_name"] for c in (t.get("table_columns") or [])],
                    )
                    for obj in objects.read_all().to_pylist()
                    for s in obj["catalog_db_schemas"]
                    for t in s["db_schema_tables"]
                )

            def names_and_types(types_filter=None):
                return [(name, kind) for name, kind, _ in listed(types_filter)]

            # Every table is listed with its type, temporary views included.
            assert names_and_types() == sorted(expected.items())
            assert names_and_types(["MANAGED"]) == [(managed, "MANAGED")]
            assert names_and_types(["VIEW"]) == sorted(
                (n, "VIEW") for n in (view, temp_view)
            )
            if has_external:
                assert names_and_types(["EXTERNAL"]) == [(external, "EXTERNAL")]
            # TEMPORARY isn't a type, so GetTableTypes doesn't list it.
            types = conn.adbc_get_table_types()
            assert {"MANAGED", "VIEW"} <= set(types)
            assert "TEMPORARY" not in types
            # At depth ALL, temporary views list their columns too.
            columns = {name: cols for name, _, cols in listed(depth="all")}
            assert columns[temp_view] == ["id"]
            assert columns[view] == ["id"]
            assert columns[managed] == ["id"]
        finally:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DROP VIEW IF EXISTS {driver.quote_one_identifier(temp_view)}"
                )
                cursor.execute(f"DROP VIEW IF EXISTS {qualified(view)}")
                cursor.execute(
                    driver.drop_table(table_name=managed, schema_name=schema)
                )
                if has_external:
                    cursor.execute(
                        driver.drop_table(table_name=external, schema_name=schema)
                    )

    def test_get_objects_name_patterns(self, driver, conn) -> None:
        if driver.short_version.startswith("emr-"):
            pytest.skip("EMR tables need an S3 location")
        # ADBC patterns use SQL LIKE syntax, which Spark's SHOW ... LIKE doesn't.
        suffix = uuid.uuid4().hex[:8]
        tables = [f"pattern_{suffix}_a", f"pattern_{suffix}xa", f"other_{suffix}"]
        catalog = driver.features.current_catalog
        schema = driver.features.current_schema

        def names(table_filter, schema_filter=schema, catalog_filter=catalog):
            objects = conn.adbc_get_objects(
                depth="tables",
                catalog_filter=catalog_filter,
                db_schema_filter=schema_filter,
                table_name_filter=table_filter,
            )
            return sorted(
                t["table_name"]
                for obj in objects.read_all().to_pylist()
                for s in obj["catalog_db_schemas"]
                for t in s["db_schema_tables"]
            )

        def columns(column_filter):
            objects = conn.adbc_get_objects(
                depth="columns",
                catalog_filter=catalog,
                db_schema_filter=schema,
                table_name_filter=tables[2],
                column_name_filter=column_filter,
            )
            return [
                c["column_name"]
                for obj in objects.read_all().to_pylist()
                for s in obj["catalog_db_schemas"]
                for t in s["db_schema_tables"]
                for c in t["table_columns"]
            ]

        with conn.cursor() as cursor:
            for table in tables:
                cursor.execute(
                    f"CREATE TABLE {driver.quote_one_identifier(table)} (id INT)"
                )
        try:
            assert names(f"pattern_{suffix}%") == sorted(tables[:2])
            # '_' matches any one character, and matching ignores case.
            assert names(f"PATTERN_{suffix}_A") == sorted(tables[:2])
            assert names(f"%_{suffix}") == [tables[2]]
            # '__' matches any two characters.
            assert names(f"pattern__{suffix[1:]}%") == sorted(tables[:2])
            # '.', '*', and '|' are literal characters.
            assert names(f"pattern.{suffix}%") == []
            assert names(f"pattern_{suffix}*") == []
            assert names(f"other_{suffix}|pattern%") == []
            # Catalog and schema patterns use the same syntax.
            assert names(
                f"%{suffix}%", schema[:-1] + "%", catalog[:-1] + "_"
            ) == sorted(tables)
            # So do column patterns.
            assert columns("I_") == ["id"]
            assert columns("i.") == []
            assert columns("id|x") == []
        finally:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(driver.drop_table(table_name=table))

    def test_get_objects_non_ascii_name_pattern(self, driver, conn) -> None:
        # Spark ignores the case of ASCII letters only in name patterns, so this
        # pattern must be matched on the client. Tables in the session catalog
        # can't have such a name, but temporary views can.
        view = f"übersicht_{uuid.uuid4().hex[:8]}"
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE TEMPORARY VIEW {driver.quote_one_identifier(view)} AS SELECT 1 AS id"
            )
        try:
            objects = conn.adbc_get_objects(
                depth="tables",
                catalog_filter="spark_catalog",
                db_schema_filter="default",
                table_name_filter=view.upper(),
            )
            tables = [
                t["table_name"]
                for obj in objects.read_all().to_pylist()
                for s in obj["catalog_db_schemas"]
                for t in s["db_schema_tables"]
            ]
            assert tables == [view]
        finally:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DROP VIEW IF EXISTS {driver.quote_one_identifier(view)}"
                )


def test_call_headers_unsupported(driver, conn, db_kwargs, driver_path):
    if driver.short_version.endswith("-connect"):
        pytest.skip("test is only for non-Connect backends")
    option = "spark.connect.call_header.x-route"
    with pytest.raises(adbc_driver_manager.NotSupportedError):
        adbc_driver_manager.AdbcDatabase(
            driver=driver_path, **{**db_kwargs, option: "private"}
        )
    with adbc_driver_manager.AdbcDatabase(driver=driver_path, **db_kwargs) as db:
        for handle in (db, conn.adbc_connection):
            with pytest.raises(adbc_driver_manager.NotSupportedError):
                handle.set_options(**{option: "private"})
            with pytest.raises(adbc_driver_manager.NotSupportedError):
                handle.get_option(option)
