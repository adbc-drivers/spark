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

set -euo pipefail

main() {
    local -r environment="${FOUNDRY_GH_ENVIRONMENT:-}"

    if [[ "$environment" == "aws-emr-serverless" ]]; then
        echo "Setting up AWS EMR Serverless environment"

        local response=$(aws emr-serverless start-session \
                             --application-id "${EMR_SERVERLESS_APPLICATION_ID}" \
                             --execution-role-arn "${EMR_SERVERLESS_EXECUTION_ROLE_ARN}")
        local session_id=$(echo "$response" | jq -r '.sessionId')
        echo "Got session ID: $session_id"

        while true; do
            local status=$(aws emr-serverless get-session \
                               --application-id "${EMR_SERVERLESS_APPLICATION_ID}" \
                               --session-id "$session_id" \
                               | jq -r '.session.state')

            if [[ "$status" == "STARTING" ]] || [[ "$status" == "SUBMITTED" ]]; then
                echo "Session is $status, waiting..."
                sleep 5
            elif [[ "$status" == "IDLE" ]] || [[ "$status" == "STARTED" ]]; then
                echo "Session is ready"
                break
            else
                echo "Session failed to start with status: $status"
                exit 1
            fi
        done

        local session_endpoint=$(aws emr-serverless get-session-endpoint \
                                     --application-id "${EMR_SERVERLESS_APPLICATION_ID}" \
                                     --session-id "$session_id")
        local host=$(echo "$session_endpoint" | jq -r '.endpoint' | sed 's|^https://||')
        local token=$(echo "$session_endpoint" | jq -r '.authToken')
        local -r uri="spark://:$token@$host:443?tls=true&auth_type=token&api=connect"
        echo "export SPARK_CONNECT_URI=\"$uri\"" >> ".env.ci"

        exit 0
    else
        echo "export AWS_ENDPOINT_URL_S3=\"http://localhost:9000\"" >> ".env.ci"
        echo "export AWS_ACCESS_KEY_ID=admin" >> ".env.ci"
        echo "export AWS_SECRET_ACCESS_KEY=password" >> ".env.ci"
    fi
}

main "$@"
