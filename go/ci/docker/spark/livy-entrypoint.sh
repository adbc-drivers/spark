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

echo "=== Livy Server Startup ==="

# Wait for keytab
echo "Waiting for keytab..."
while [ ! -f /var/keytabs/hive.keytab ]; do
    echo "Keytab not found, waiting..."
    sleep 2
done

# Initialize Kerberos credentials
echo "Initializing Kerberos credentials..."
kinit -V -k -t /var/keytabs/hive.keytab hiveuser/hive-metastore@KDC.LOCAL

echo "Kerberos credentials obtained successfully:"
klist

exec $LIVY_HOME/bin/livy-server
