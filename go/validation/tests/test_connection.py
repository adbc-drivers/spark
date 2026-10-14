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

import uuid

import adbc_drivers_validation.tests.connection
import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = [spark.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


class TestConnection(adbc_drivers_validation.tests.connection.TestConnection):
    def test_get_objects_name_patterns(self, driver, conn) -> None:
        if "thrift" in driver.short_version:
            pytest.skip("Thrift reads '.', '*', and '|' in patterns as regex (#104)")
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
            # '.', '*', and '|' are literal characters.
            assert names(f"pattern.{suffix}%") == []
            assert names(f"pattern_{suffix}*") == []
            assert names(f"other_{suffix}|pattern%") == []
            # Catalog and schema patterns use the same syntax.
            assert names(
                f"%{suffix}%", schema[:-1] + "%", catalog[:-1] + "_"
            ) == sorted(tables)
        finally:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(driver.drop_table(table_name=table))
