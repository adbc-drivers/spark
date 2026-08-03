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
    # Terminate the session ID post-test in CI so that resources are freed for
    # any runs that occur right after. If this fails, it's OK since the
    # session will also expire, but any runs until then may spuriously fail
    # due to the existing session hogging resources.
    if [[ -n "${EMR_SESSION_ID:-}" ]]; then
        aws emr-serverless terminate-session \
            --application-id "${EMR_SERVERLESS_APPLICATION_ID}" \
            --session-id "${EMR_SESSION_ID}"
    fi
}

main "$@"
