// Copyright (c) 2026 ADBC Drivers Contributors
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

import (
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestBulkIngestCreateTableStatementLocation(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	ingestOptions := NewBulkIngestOptions()
	ingestOptions.TableName = "target"
	ingestOptions.location = `s3://bucket/path'segment\part`
	bi := bulkIngestImpl{
		options: ingestOptions,
	}

	stmts, err := bi.createTableStatement(schema, driverbase.BulkIngestTableExistsError, driverbase.BulkIngestTableMissingCreate)

	require.NoError(t, err)
	require.Equal(t, []string{`CREATE TABLE ` + "`target`" + ` (` + "`id`" + ` INTEGER) LOCATION 's3://bucket/path\'segment\\part'`}, stmts)
}
