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

import re

import adbc_driver_manager.dbapi
import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = spark.get_quirks(metafunc.config.getoption("vendor_version"))
    driver_param = f"{quirks.name}:{quirks.short_version}"
    if quirks.short_version.endswith("-livy"):
        combinations = [pytest.param(driver_param, id=driver_param)]
    else:
        combinations = [
            pytest.param(
                driver_param,
                id=driver_param,
                marks=[pytest.mark.skip(reason="test is only for Livy")],
            )
        ]
    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


def test_session_id(driver, conn, db_kwargs, driver_path):
    session_id = conn.adbc_connection.get_option("spark.livy.session_id")
    assert re.match(r"^\d+$", session_id), f"Invalid session ID: {session_id}"

    with adbc_driver_manager.dbapi.connect(
        driver_path,
        db_kwargs={
            **db_kwargs,
            "spark.livy.session_id": session_id,
            "spark.livy.delete_session": False,
        },
        autocommit=True,
    ) as conn2:
        session_id2 = conn2.adbc_connection.get_option("spark.livy.session_id")
        assert session_id == session_id2

        with conn2.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchall()
            assert result == [(1,)]
