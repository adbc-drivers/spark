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

import os

import adbc_driver_manager.dbapi
import pytest

from . import spark


def pytest_generate_tests(metafunc) -> None:
    quirks = spark.get_quirks(metafunc.config.getoption("vendor_version"))
    driver_param = f"{quirks.name}:{quirks.short_version}"
    combinations = [pytest.param(driver_param, id=driver_param)]
    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


def test_auth(subtests, driver, driver_path):
    all_options = {
        f"auth_type={t}"
        for t in [
            "aws_sigv4",
            "basic",
            "ldap",
            "kerberos",
            "none",
            "nosasl",
            "plain",
            "token",
        ]
    }

    if driver.short_version.endswith("-thrifthttp") or driver.short_version.endswith(
        "-thrift"
    ):
        # ensure none leads to auth failure, and that all other types are not accepted
        if driver.short_version.endswith("-thrift"):
            uri = os.environ["SPARK_URI"]
        else:
            uri = os.environ["SPARK_THRIFTHTTP_URI"]
        orig = "auth_type=plain"
        cases = [
            ("auth_type=nosasl", "Could not open HiveServer2 session"),
            ("auth_type=ldap", "auth type 'ldap' has not been implemented"),
            ("auth_type=kerberos", "auth type 'kerberos' has not been implemented"),
        ]
    elif driver.short_version.endswith("-connect"):
        uri = os.environ["SPARK_CONNECT_URI"]
        orig = "auth_type=none"
        cases = [
            # Spark Connect client forces TLS
            ("auth_type=token", "Could not execute query"),
        ]
    elif driver.short_version.endswith("-livy"):
        uri = os.environ["SPARK_LIVY_URI"]
        orig = "auth_type=basic"
        cases = [
            ("auth_type=aws_sigv4", "failed to sign request"),
        ]
    else:
        raise NotImplementedError(driver.short_version)

    for option in all_options:
        seen = set([orig] + [c[0] for c in cases])
        if option not in seen:
            cases.append(
                (
                    option,
                    f"invalid option value '{option[10:]}' for option spark.auth_type",
                )
            )
    cases.sort(key=lambda c: c[0])

    for replacement, error_message in cases:
        new_uri = uri.replace(orig, replacement)
        if replacement == "auth_type=nosasl":
            kwargs = {}
        else:
            kwargs = {
                "username": "spark",
                "password": "spark",
            }

        with subtests.test(auth_type=replacement[10:]):
            with pytest.raises(adbc_driver_manager.Error, match=error_message):
                with adbc_driver_manager.dbapi.connect(
                    driver=driver_path,
                    uri=new_uri,
                    autocommit=True,
                    db_kwargs=kwargs,
                ) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
