#!/bin/bash
set -e
set -x

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

echo "INITIALIZING SCHEMA"
${HIVE_HOME}/bin/schematool -dbType postgres -initSchema || true

echo "STARTING HIVE"
exec ${HIVE_HOME}/bin/hive --service metastore --verbose
