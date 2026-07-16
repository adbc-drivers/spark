#!/bin/bash
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

# Bootstrap catalogs before Spark starts by running SPARK_INIT_SQL

set -exo pipefail

while [ ! -f /var/keytabs/hive.keytab ]; do
    sleep 2
done

kinit -V -k -t /var/keytabs/hive.keytab hiveuser/hive-metastore@KDC.LOCAL

exec "${SPARK_HOME}/bin/spark-sql" \
    --properties-file "${SPARK_HOME}/conf/spark-defaults.conf" \
    -e "${SPARK_INIT_SQL}"
