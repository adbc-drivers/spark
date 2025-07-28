// Copyright (c) 2025 Columnar Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spark

const (
	// StatementOptionIngestStagingAreaURI specifies the staging area when
	// ingesting data.  It may also be set at the database level.
	// Depending on the driver, this can be something like an S3 URI or
	// file:/// URI.  The database may require additional configuration to
	// be able to use the staging area.  Consult your vendor's
	// documentation.
	StatementOptionIngestStagingAreaURI = "spark.ingest.staging_area_uri"
)
