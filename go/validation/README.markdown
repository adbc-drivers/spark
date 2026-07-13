<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Validation Suite Setup

Set up necessary environment variables:

```bash
# If you intend to test Amazon EMR: set these variables appropriately
# export EMR_SERVERLESS_APPLICATION_ID=...
# export EMR_SERVERLESS_EXECUTION_ROLE_ARN=...
# export STAGING_S3_BUCKET=...

./ci/scripts/pre-test.sh
source .env.linux
source .env.ci
```
