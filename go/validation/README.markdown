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

## Setting up EMR Serverless

- Your role needs S3 Tables access.
- You need this application configuration:

  ```
  {
    "runtimeConfiguration": [
      {
        "classification": "spark-defaults",
        "configurations": null,
        "properties": {
          "spark.sql.catalog.ghatestcatalog": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalogImplementation": "hive",
          "spark.sql.catalog.ghatestcatalog.rest.signing-name": "glue",
          "spark.sql.catalog.ghatestcatalog.rest-metrics-reporting-enabled": "false",
          "spark.sql.catalog.ghatestcatalog.rest.sigv4-enabled": "true",
          "spark.sql.catalog.ghatestcatalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
          "spark.sql.catalog.ghatestcatalog.uri": "https://glue.us-east-1.amazonaws.com/iceberg",
          "spark.sql.catalog.ghatestcatalog.warehouse": "<ACCOUNT ID>:<S3 TABLES BUCKET NAME>/<CATALOG NAME>",
          "spark.sql.catalog.ghatestcatalog.type": "rest",
          "spark.sql.catalog.ghatestcatalog.rest.signing-region": "us-east-1"
        }
      }
    ]
  }
  ```
