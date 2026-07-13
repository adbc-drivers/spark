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

import adbc_drivers_validation.tests.statement
import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = [spark.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.statement.generate_tests(quirks, metafunc)


class TestStatement(adbc_drivers_validation.tests.statement.TestStatement):
    def test_rows_affected(self, driver, conn):
        if driver.short_version.startswith("4.0-"):
            pytest.skip("Spark 4 does not report UPDATE/DELETE row count")
        if driver.short_version.startswith("3.5-"):
            pytest.skip(
                "Spark 3.5 returns -1 for rows affected instead of actual count"
            )
        if driver.short_version.startswith("emr-"):
            pytest.skip("EMR Spark does not support UPDATE etc.")
        super().test_rows_affected(driver, conn)
