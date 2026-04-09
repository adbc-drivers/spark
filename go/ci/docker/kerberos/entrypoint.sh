#!/bin/bash

set -e
set -x

# Create master database
if [ ! -f "/var/lib/krb5kdc/principal" ]; then
        kdb5_util create -r "$REALM" -s -P "$KADMIN_PASSWORD"

        KEYTABS_PATH=/var/keytabs
        find $KEYTABS_PATH -name "*.keytab" -delete

        kadmin.local -q "delete_principal -force $KADMIN_PRINCIPAL"
        kadmin.local -q "addprinc -pw adminpassword $KADMIN_PRINCIPAL"

        HIVE_PRINCIPAL="hiveuser/hive-metastore@$REALM"
        kadmin.local -q "delete_principal -force $HIVE_PRINCIPAL"
        kadmin.local -q "addprinc -pw hivepassword $HIVE_PRINCIPAL"
        kadmin.local -q "ktadd -k $KEYTABS_PATH/hive.keytab $HIVE_PRINCIPAL"

        # Verify keytab was created successfully
        if [ ! -f "$KEYTABS_PATH/hive.keytab" ]; then
            echo "ERROR: Failed to create hive.keytab"
            exit 1
        fi

        echo "Keytab created successfully: $KEYTABS_PATH/hive.keytab"
        ls -la $KEYTABS_PATH/

        chmod -R ugo+rw $KEYTABS_PATH/
fi

krb5kdc
kadmind -nofork
