---
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
{}
---

{{ cross_reference|safe }}
# Apache Spark Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Apache Spark][spark] (commonly referred to
as just "Spark").

:::{note}
This project is not affiliated with the Apache Software Foundation.
:::

## Installation & Quickstart

The driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install spark --pre
```

:::{note}
Only prerelease versions of the driver are currently available, so you must use `--pre` with dbc 0.2.0 or newer to install the driver.
:::

## Connecting

To use the driver, provide a connection string as the `uri` option.

```python
from adbc_driver_manager import dbapi

dbapi.connect(
  driver="spark",
  db_kwargs={
      "uri": "spark://localhost:10000?auth_type=plain&api=thrift%2Bbinary"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.  See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

- The URI scheme is "spark://".
- The host and port should be provided.
- If not specified, the `api` defaults to `thrift+binary` (URI-encoded: `thrift%2Bbinary`).
- Options can be specified as query parameters or as driver options.

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40` and `+` becomes `%2B`.
:::

### Connection Options

These parameters can be specified in the URI as query parameters, or as connection parameters:

`spark.api` (query parameter: `api`)
: **Values**: `connect`, `livy`, `thrift+binary`, or `thrift+http`.

  The protocol used to connect to Spark.

  | Value           | Backend                        |
  |-----------------|--------------------------------|
  | `connect`       | Spark Connect                  |
  | `livy`          | Apache Livy                    |
  | `thrift+binary` | HiveServer2 Thrift (over TCP)  |
  | `thrift+http`   | HiveServer2 Thrift (over HTTP) |

`spark.auth_type` (query parameter: `auth_type`)
: **Values**: `sql`, `spark`, or `pyspark`.

  How to authenticate to Spark.

  | Auth Type   | Applicable Backends            | Description               |
  |-------------|--------------------------------|---------------------------|
  | `aws_sigv4` | `livy`                         | Use AWS SDK               |
  | `basic`     | `livy`                         | Username/password         |
  | `ldap`      | `thrift+binary`, `thrift+http` | Not yet implemented       |
  | `kerberos`  | `thrift+binary`, `thrift+http` | Not yet implemented       |
  | `none`      | `connect`, `livy`              | No authentication         |
  | `nosasl`    | `thrift+binary`, `thrift+http` | No authentication         |
  | `plain`     | `thrift+binary`, `thrift+http` | Username/password         |
  | `token`     | `connect`                      | Username/password (token) |

`spark.livy.session_kind` (query parameter: `livy.session_kind`)
: **Values**: `sql`, `spark`, or `pyspark`.

  For the Livy backend, what kind of session to create.

  :::{warning}
  Currently only `sql` is tested/supported.
  :::

`spark.tls` (query parameter: `tls`)
: **Type** boolean. **Default**: false.

  Whether to use TLS for connecting. Only applies to `connect`, `livy`, and `thrift+http`.

`spark.validate_server_certificate` (query parameter: `validateservercertificate`)
: **Type** boolean. **Default**: true.

  Whether to validate the server's TLS certificate. Should only be disabled for development/testing.

## Limitations

Different backends and cluster configurations have limitations; some limitations related to data type support are also noted further below.

### HiveServer2/Thrift Protocol

- In Spark 3.x, binary data that does not happen to be valid UTF-8 will be corrupted.
- The client cannot tell whether a timestamp carries a time zone or not; all timestamps are assumed to be in UTC as a result.

### Apache Livy

- Only the first 1000 rows of a result set can be fetched. This can be tuned by configuring Spark with `spark.sql.repl.eagerEval.maxNumRows`.
- In general, we have found that performance is worse than with Spark Connect or HiveServer2.
- Connecting to an Amazon EMR (Serverless) cluster via Livy requires setting the `emr-serverless.session.executionRoleArn` session config option to an appropriate role ARN. This can be set via the ADBC option `spark.opt.emr-serverless.session.executionRoleArn`.
- By default, the driver will attempt to start a new Livy session, which tends to take some time (~a few minutes). To amortize this time across multiple connections, the option `spark.livy.session_id` can be used to fetch the session ID, and to provide it upon connection, bypassing creating a new session.
- By default, the driver will close the session when the connection is closed. Setting `spark.livy.delete_session` to `false` on connection will avoid this, making it easier to reuse the session.

### Spark Connect

- The connection URI should look like this:

  ```
  spark://:<AUTH TOKEN>@<SESSION ID>.s.emr-serverless-services.<REGION>.amazonaws.com:443?tls=true&auth_type=token&api=connect
  ```

  The full hostname can be obtained from the AWS API, e.g. via the CLI:

  ```
  aws emr-serverless get-session-endpoint --application-id <APPLICATION ID> --session-id <SESSION ID>
  ```

  This command will also give you the auth token.

### Amazon EMR (Serverless)

- Bulk ingest with an AWS Glue catalog is not currently supported as there is no way to specify the `LOCATION` clause.
- Amazon EMR is not currently enabled in our automated integration testing.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Compatibility

{{ compatibility_info|safe }}

[spark]: https://spark.apache.org/
