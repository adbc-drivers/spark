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

echo "=== Spark Thrift Server Startup ==="

# Wait for keytab
echo "Waiting for keytab..."
while [ ! -f /var/keytabs/hive.keytab ]; do
    echo "Keytab not found, waiting..."
    sleep 2
done

# Verify keytab exists and is readable
if [ ! -r /var/keytabs/hive.keytab ]; then
    echo "ERROR: Keytab exists but is not readable"
    ls -la /var/keytabs/
    exit 1
fi

# Initialize Kerberos credentials
echo "Initializing Kerberos credentials..."
kinit -V -k -t /var/keytabs/hive.keytab hiveuser/hive-metastore@KDC.LOCAL

# Verify credentials were obtained
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to obtain Kerberos credentials"
    exit 1
fi

# Display credentials for debugging
echo "Kerberos credentials obtained successfully:"
klist

# Export environment variables for Hadoop Kerberos authentication
export HADOOP_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf -Djava.security.auth.login.config=$SPARK_HOME/conf/jaas.conf"

# Start Spark Thrift Server
echo "Starting Spark Thrift Server..."
$SPARK_HOME/sbin/start-thriftserver.sh \
  --properties-file $SPARK_HOME/conf/spark-defaults.conf \
  --conf spark.yarn.keytab=/var/keytabs/hive.keytab \
  --conf spark.yarn.principal=hiveuser/hive-metastore@KDC.LOCAL

# Wait for log file to be created
echo "Waiting for Thrift Server to start..."
for i in {1..30}; do
    if ls $SPARK_HOME/logs/*thriftserver*.out 2>/dev/null; then
        break
    fi
    sleep 1
done

# Tail the log file to keep container running
echo "Thrift Server started, tailing logs..."
exec tail -f $SPARK_HOME/logs/*thriftserver*.out
