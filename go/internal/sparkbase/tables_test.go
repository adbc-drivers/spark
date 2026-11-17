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

package sparkbase

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// showTablesClient answers the SHOW TABLES query with fixed rows. Other
// SparkClient methods aren't used, so they're left to the embedded nil interface.
type showTablesClient struct {
	SparkClient
	schema *arrow.Schema
	rows   [][]any
	query  string
}

func (c *showTablesClient) ExecuteQuery(_ context.Context, query QueryContext) (array.RecordReader, int64, error) {
	c.query = query.Query
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, c.schema)
	defer bldr.Release()
	for _, row := range c.rows {
		for i, v := range row {
			if v == nil {
				bldr.Field(i).AppendNull()
				continue
			}
			switch b := bldr.Field(i).(type) {
			case *array.StringBuilder:
				b.Append(v.(string))
			case *array.BooleanBuilder:
				b.Append(v.(bool))
			}
		}
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()
	rr, err := array.NewRecordReader(c.schema, []arrow.RecordBatch{rec})
	return rr, 0, err
}

func showTablesSchema(withTemporary bool) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "namespace", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "tableName", Type: arrow.BinaryTypes.String, Nullable: false},
	}
	if withTemporary {
		fields = append(fields, arrow.Field{Name: "isTemporary", Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}

func tableTypesOf(t *testing.T, client SparkClient, tableFilter *string) map[string]string {
	t.Helper()
	tables, err := DefaultGetTablesForDBSchemaImpl(client, context.Background(), "spark_catalog", "default", tableFilter, nil, false)
	require.NoError(t, err)
	types := map[string]string{}
	for _, table := range tables {
		types[table.TableName] = table.TableType
	}
	return types
}

func TestGetTablesReportsTemporaryViews(t *testing.T) {
	client := &showTablesClient{
		schema: showTablesSchema(true),
		rows: [][]any{
			{"default", "events", false},
			{"", "session_view", true},
			{"default", "unknown_temp", nil},
		},
	}
	require.Equal(t, map[string]string{
		"events":       TableTypeTable,
		"session_view": TableTypeTemporary,
		"unknown_temp": TableTypeTable,
	}, tableTypesOf(t, client, nil))
}

func TestGetTablesWithoutTemporaryColumn(t *testing.T) {
	// Without an isTemporary column every table is a TABLE, as before.
	client := &showTablesClient{
		schema: showTablesSchema(false),
		rows:   [][]any{{"default", "events"}, {"", "session_view"}},
	}
	require.Equal(t, map[string]string{
		"events":       TableTypeTable,
		"session_view": TableTypeTable,
	}, tableTypesOf(t, client, nil))
}

func TestGetTablesNameFilterAppliesToTemporaryViews(t *testing.T) {
	client := &showTablesClient{
		schema: showTablesSchema(true),
		rows: [][]any{
			{"default", "events", false},
			{"", "session_view", true},
		},
	}
	filter := "session%"
	require.Equal(t, map[string]string{
		"session_view": TableTypeTemporary,
	}, tableTypesOf(t, client, &filter))
	require.Contains(t, client.query, "SHOW TABLES")
}
