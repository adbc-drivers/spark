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

set -e
set -x

METASTORE_DB_NAME="${METASTORE_DB_NAME:-metastore}"
METASTORE_WAREHOUSE_DIR="${METASTORE_WAREHOUSE_DIR:-/opt/spark-data/warehouse}"
METASTORE_DB_URL="jdbc:postgresql://metastore-db:5432/${METASTORE_DB_NAME}"

# Wait for keytab to exist
echo "Waiting for keytab..."
while [ ! -f /var/keytabs/hive.keytab ]; do
    sleep 1
done

# Initialize Kerberos credentials
echo "Initializing Kerberos credentials..."
kinit -V -k -t /var/keytabs/hive.keytab hiveuser/hive-metastore@KDC.LOCAL
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to obtain Kerberos credentials"
    exit 1
fi

# Verify credentials
echo "Verifying Kerberos credentials..."
klist

echo "ENSURING DATABASE EXISTS"
if ! PGPASSWORD=hive psql -h metastore-db -U hive -d metastore -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '${METASTORE_DB_NAME}'" | grep -q 1; then
    PGPASSWORD=hive createdb -h metastore-db -U hive "${METASTORE_DB_NAME}"
fi

echo "INITIALIZING SCHEMA"
${HIVE_HOME}/bin/schematool \
    -dbType postgres \
    -url "${METASTORE_DB_URL}" \
    -userName hive \
    -passWord hive \
    -initSchema || true

echo "STARTING HIVE"
exec ${HIVE_HOME}/bin/hive \
    --service metastore \
    --hiveconf javax.jdo.option.ConnectionURL="${METASTORE_DB_URL}" \
    --hiveconf hive.metastore.warehouse.dir="${METASTORE_WAREHOUSE_DIR}" \
    --verbose
