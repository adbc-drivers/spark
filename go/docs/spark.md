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
dbc install spark
```

## Connecting

To use the driver, provide an Oracle connection string as the `uri` option. The driver supports URI format and DSN-style connection strings, but URIs are recommended.

```python
from adbc_driver_manager import dbapi

dbapi.connect(
  driver="spark",
  db_kwargs={
      "uri": ""
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.  See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

## Limitations

### Hiveserver2/Thrift Protocol

- In Spark 3.x, binary data that does not happen to be valid UTF-8 will be corrupted.
- The client cannot tell whether a timestamp carries a time zone or not; all timestamps are assumed to be in UTC as a result.

### Livy

- Only the first 1000 rows of a result set can be fetched. This can be tuned by configuring Spark with `spark.sql.repl.eagerEval.maxNumRows`.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Compatibility

{{ compatibility_info|safe }}
