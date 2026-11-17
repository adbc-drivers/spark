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

    def test_execute_plan_passthrough(self, driver, conn, db_kwargs):
        if not driver.short_version.startswith("4."):
            pytest.skip("only test ExecutePlan passthrough on Spark 4.x")
        if not driver.short_version.endswith("-connect"):
            pytest.skip("ExecutePlan passthrough is only supported in Spark Connect")

        from pyspark.sql import SparkSession
        from pyspark.sql.connect.proto import ExecutePlanRequest, ExecutePlanResponse

        url = urllib.parse.urlparse(db_kwargs["uri"])
        spark = SparkSession.builder.remote(f"sc://{url.netloc}").getOrCreate()
        plan = spark.range(10).filter("id >= 5")._plan.to_proto(spark.client)
        request = ExecutePlanRequest(plan=plan, tags=["adbc-execute-plan-test"])
        params = pyarrow.RecordBatch.from_arrays(
            [pyarrow.array([request.SerializeToString()], type=pyarrow.binary())],
            schema=pyarrow.schema(
                [pyarrow.field("request", pyarrow.binary(), nullable=False)]
            ),
        )
        responses = []
        with conn.cursor() as cur:
            cur.adbc_statement.set_options(
                **{"adbc.action": "spark.connect.execute_plan"}
            )
            cur.adbc_statement.bind_stream(
                pyarrow.RecordBatchReader.from_batches(params.schema, [params])
            )
            operation_id = cur.adbc_statement.get_option("spark.connect.operation_id")
            assert operation_id
            handle, _ = cur.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                assert reader.schema == pyarrow.schema(
                    [pyarrow.field("response", pyarrow.binary(), nullable=False)]
                )
                for batch in reader:
                    assert batch.num_rows == 1
                    assert batch.column(0).null_count == 0
                    response = ExecutePlanResponse.FromString(
                        batch.column(0)[0].as_py()
                    )
                    assert response.operation_id == operation_id
                    responses.append(response)

        assert responses
        assert any(response.HasField("schema") for response in responses)
        assert any(response.HasField("metrics") for response in responses)
        assert any(not response.HasField("arrow_batch") for response in responses)
        tables = []
        for response in responses:
            if response.HasField("arrow_batch"):
                with pyarrow.ipc.open_stream(response.arrow_batch.data) as reader:
                    table = reader.read_all()
                assert table.num_rows == response.arrow_batch.row_count
                tables.append(table)
        assert tables
        result = pyarrow.concat_tables(tables)
        assert result.to_pydict() == {"id": [5, 6, 7, 8, 9]}

    def test_analyze_plan_passthrough(self, unary_relay):
        from pyspark.sql.connect import proto

        response = unary_relay(
            "analyze_plan",
            proto.AnalyzePlanRequest(
                spark_version=proto.AnalyzePlanRequest.SparkVersion()
            ),
            proto.AnalyzePlanResponse,
        )
        assert response.spark_version.version.startswith("4.")

    def test_config_passthrough(self, unary_relay):
        from pyspark.sql.connect import proto

        key = "adbc.d2.test"

        def config(operation):
            return unary_relay(
                "config", proto.ConfigRequest(operation=operation), proto.ConfigResponse
            )

        try:
            config(
                proto.ConfigRequest.Operation(
                    set=proto.ConfigRequest.Set(
                        pairs=[proto.KeyValue(key=key, value="")]
                    )
                )
            )
            response = config(
                proto.ConfigRequest.Operation(get=proto.ConfigRequest.Get(keys=[key]))
            )
            assert response.pairs[0].HasField("value")
            assert response.pairs[0].value == ""
        finally:
            config(
                proto.ConfigRequest.Operation(
                    unset=proto.ConfigRequest.Unset(keys=[key])
                )
            )
        response = config(
            proto.ConfigRequest.Operation(
                get_option=proto.ConfigRequest.GetOption(keys=[key])
            )
        )
        assert not response.pairs[0].HasField("value")

    def test_artifact_status_passthrough(self, unary_relay):
        from pyspark.sql.connect import proto

        names = ["cache/adbc-d2-missing-a", "cache/adbc-d2-missing-b"]
        response = unary_relay(
            "artifact_status",
            proto.ArtifactStatusesRequest(names=names),
            proto.ArtifactStatusesResponse,
        )
        assert set(response.statuses) == set(names)
        assert all(not status.exists for status in response.statuses.values())

    def test_interrupt_passthrough(self, unary_relay):
        from pyspark.sql.connect import proto

        for kind, target in [
            (1, {}),
            (2, {"operation_tag": "adbc-d2-missing"}),
            (3, {"operation_id": "00000000-0000-0000-0000-000000000000"}),
        ]:
            response = unary_relay(
                "interrupt",
                proto.InterruptRequest(interrupt_type=kind, **target),
                proto.InterruptResponse,
            )
            assert not response.interrupted_ids


@pytest.fixture
def unary_relay(driver, conn):
    if not (
        driver.short_version.startswith("4.")
        and driver.short_version.endswith("-connect")
    ):
        pytest.skip("unary passthrough requires Spark 4.x Connect")

    def relay(action, request, response_type):
        schema = pyarrow.schema(
            [pyarrow.field("request", pyarrow.binary(), nullable=False)]
        )
        batch = pyarrow.RecordBatch.from_arrays(
            [pyarrow.array([request.SerializeToString()], type=pyarrow.binary())],
            schema=schema,
        )
        with conn.cursor() as cur:
            cur.adbc_statement.set_options(**{"adbc.action": "spark.connect." + action})
            cur.adbc_statement.bind_stream(
                pyarrow.RecordBatchReader.from_batches(schema, [batch])
            )
            handle, _ = cur.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                assert reader.schema == pyarrow.schema(
                    [pyarrow.field("response", pyarrow.binary(), nullable=False)]
                )
                table = reader.read_all()
            assert table.num_rows == 1
            return response_type.FromString(table.column(0)[0].as_py())

    return relay
