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
