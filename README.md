<!--
  Copyright (c) 2025-2026 ADBC Drivers Contributors

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

# ADBC Drivers for Apache Spark

This project is not affiliated with the Apache Software Foundation.

This repository contains [ADBC drivers](https://arrow.apache.org/adbc/) for
Apache Spark, implemented in different languages.

## Installation

Pre-packaged builds of the drivers in this repo have been made available for
various platforms from the [Columnar](https://columnar.tech) CDN. These can be
installed by any tool that supports [ADBC](https://arrow.apache.org/adbc/)
Driver Manifests, such as [dbc](https://columnar.tech/dbc):

```sh
dbc install spark
```

> [!NOTE]
> Only prerelease versions of the driver are currently available, so you must use `--pre` with dbc 0.2.0 or newer to install the driver.

See [Building](#building) if you would rather build the drivers yourself.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
