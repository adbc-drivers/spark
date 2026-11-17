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

import urllib.parse

import adbc_driver_manager.dbapi
import adbc_drivers_validation.tests.statement
import pyarrow
import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = [spark.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.statement.generate_tests(quirks, metafunc)


class TestStatement(adbc_drivers_validation.tests.statement.TestStatement):
    def test_rows_affected(self, driver, conn):
        if driver.short_version.startswith("4."):
            pytest.skip("Spark 4 does not report UPDATE/DELETE row count")
        if driver.short_version.startswith("3.5-"):
            pytest.skip(
                "Spark 3.5 returns -1 for rows affected instead of actual count"
            )
        if driver.short_version.startswith("emr-"):
            pytest.skip("EMR Spark does not support UPDATE etc.")
        super().test_rows_affected(driver, conn)

    def test_bind_parameters_without_markers(self, driver, conn):
        # A query with no '?' markers runs as before, with or without a binding.
        with conn.cursor() as cur:
            cur.execute("SELECT 'plain' AS s", parameters=[1, 2])
            assert cur.fetchall() == [("plain",)]

    def test_bind_parameters(self, driver, conn):
        # The standard bind tests cover types and multiple rows. These cover
        # what they don't: the marker rules, the uint64 limit, and backends
        # that can't bind.
        if not driver.short_version.endswith("-connect"):
            # Only Spark Connect binds parameters. Other backends say so.
            with (
                conn.cursor() as cur,
                pytest.raises(adbc_driver_manager.dbapi.NotSupportedError),
            ):
                cur.execute("SELECT ?", parameters=[1])
            return

        # A '?' inside a string literal is not a marker.
        with conn.cursor() as cur:
            cur.execute("SELECT ? AS x, '?' AS y", parameters=[42])
            assert cur.fetchall() == [(42, "?")]

        # A raw string has no backslash escapes, so its quote closes at the
        # backslash, and the marker after it counts.
        with conn.cursor() as cur:
            cur.execute("SELECT r'abc\\' AS s, ? AS v", parameters=[42])
            assert cur.fetchall() == [("abc\\", 42)]

        # A null of a typed column stays typed, so every row has the column's
        # type, and the null row isn't a null-typed result.
        with conn.cursor() as cur:
            batch = pyarrow.record_batch(
                [pyarrow.array([None, 42], type=pyarrow.int64())], names=["v"]
            )
            cur.adbc_statement.bind(batch)
            cur.adbc_statement.set_sql_query("SELECT ? AS v")
            handle, _ = cur.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()
            assert result.schema.field(0).type == pyarrow.int64()
            assert result.column(0).to_pylist() == [None, 42]

        # A uint64 above the signed maximum is rejected, not wrapped.
        with conn.cursor() as cur:
            batch = pyarrow.record_batch(
                [pyarrow.array([2**63], type=pyarrow.uint64())], names=["v"]
            )
            cur.adbc_statement.bind(batch)
            cur.adbc_statement.set_sql_query("SELECT ?")
            with pytest.raises(adbc_driver_manager.dbapi.ProgrammingError):
                cur.adbc_statement.execute_query()

        # The number of values must match the markers.
        with (
            conn.cursor() as cur,
            pytest.raises(adbc_driver_manager.dbapi.ProgrammingError),
        ):
            cur.execute("SELECT ?, ?", parameters=[1])

    def test_logical_plan_passthrough(self, driver, conn, db_kwargs):
        if not driver.short_version.startswith("4."):
            pytest.skip("only test logical plan passthrough on Spark 4.x")
        if not driver.short_version.endswith("-connect"):
            pytest.skip("logical plan passthrough is only supported in Spark Connect")

        from pyspark.sql import SparkSession

        url = urllib.parse.urlparse(db_kwargs["uri"])
        spark = SparkSession.builder.remote(f"sc://{url.netloc}").getOrCreate()
        plan = spark.range(10).filter("id >= 5")._plan.to_proto(spark.client)
        plan = plan.SerializeToString()
        with conn.cursor() as cur:
            cur.adbc_statement.set_options(**{"spark.query.logical_plan": plan})
            handle, _ = cur.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                res = reader.read_all()
        assert len(res) == 5, str(res)
