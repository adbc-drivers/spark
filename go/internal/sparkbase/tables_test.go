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
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// catalogClient answers SHOW TABLE EXTENDED with fixed rows, and DESCRIBE TABLE
// with the columns given per table reference. A reference that isn't given is
// NotFound. Other SparkClient methods are left to the embedded nil interface.
type catalogClient struct {
	SparkClient
	listSchema *arrow.Schema
	listRows   [][]any
	// extendedErr, when set, is returned by SHOW TABLE EXTENDED.
	extendedErr error
	// tablesRows are the rows of SHOW TABLES, which has no information column.
	tablesRows [][]any
	describe   map[string][][]string
	queries    []string
}

func (c *catalogClient) ExecuteQuery(_ context.Context, query QueryContext) (array.RecordReader, int64, error) {
	c.queries = append(c.queries, query.Query)
	switch {
	case strings.HasPrefix(query.Query, "SHOW TABLE EXTENDED"):
		if c.extendedErr != nil {
			return nil, 0, c.extendedErr
		}
		return recordReader(c.listSchema, c.listRows)
	case strings.HasPrefix(query.Query, "SHOW TABLES IN"):
		return recordReader(tablesSchema, c.tablesRows)
	case strings.HasPrefix(query.Query, "DESCRIBE TABLE "):
		ref := strings.TrimPrefix(query.Query, "DESCRIBE TABLE ")
		rows, ok := c.describe[ref]
		if !ok {
			return nil, 0, adbc.Error{Code: adbc.StatusNotFound, Msg: "[spark] table not found: " + ref}
		}
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "col_name", Type: arrow.BinaryTypes.String},
			{Name: "data_type", Type: arrow.BinaryTypes.String},
		}, nil)
		anyRows := make([][]any, len(rows))
		for i, row := range rows {
			anyRows[i] = []any{row[0], row[1]}
		}
		return recordReader(schema, anyRows)
	}
	return nil, 0, adbc.Error{Code: adbc.StatusNotImplemented, Msg: "unexpected query: " + query.Query}
}

// recordReader builds one record from rows. A nil value is a null, and bool and
// string values fill bool and string columns.
func recordReader(schema *arrow.Schema, rows [][]any) (array.RecordReader, int64, error) {
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()
	for _, row := range rows {
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
	rr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	return rr, -1, err
}

// listSchema is the result schema of SHOW TABLE EXTENDED.
var listSchema = arrow.NewSchema([]arrow.Field{
	{Name: "namespace", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tableName", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "isTemporary", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	{Name: "information", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// information builds the information column of SHOW TABLE EXTENDED. Its Type:
// line is omitted when tableType is "".
func information(tableType string) string {
	lines := []string{"Catalog: spark_catalog", "Table: t"}
	if tableType != "" {
		lines = append(lines, "Type: "+tableType)
	}
	return strings.Join(lines, "\n") + "\n"
}

// tablesSchema is the result schema of SHOW TABLES.
var tablesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "namespace", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tableName", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "isTemporary", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
}, nil)

// v2Error is the error Spark 3.5 returns for SHOW TABLE EXTENDED on a V2
// catalog, as Connect reports it.
var v2Error = adbc.Error{
	Code: adbc.StatusIO,
	Msg:  "[spark] Could not execute query: [_LEGACY_ERROR_TEMP_1200] SHOW TABLE EXTENDED is not supported for v2 tables. SQLSTATE: 0A000",
}

func tableTypes(t *testing.T, client *catalogClient, tableFilter *string) map[string]string {
	t.Helper()
	tables, err := DefaultGetTablesForDBSchemaImpl(client, context.Background(), "spark_catalog", "default", tableFilter, nil, false)
	require.NoError(t, err)
	types := map[string]string{}
	for _, table := range tables {
		types[table.TableName] = table.TableType
	}
	return types
}

func TestGetTablesReportsTypeFromInformation(t *testing.T) {
	client := &catalogClient{listSchema: listSchema, listRows: [][]any{
		{"default", "managed_t", false, information("MANAGED")},
		{"default", "external_t", false, information("EXTERNAL")},
		{"default", "v", false, information("VIEW")},
	}}
	require.Equal(t, map[string]string{
		"managed_t":  "MANAGED",
		"external_t": "EXTERNAL",
		"v":          "VIEW",
	}, tableTypes(t, client, nil))
	require.Equal(t, []string{"SHOW TABLE EXTENDED IN `spark_catalog`.`default` LIKE '*'"}, client.queries)
}

func TestGetTablesTemporaryViewsAreViews(t *testing.T) {
	client := &catalogClient{listSchema: listSchema, listRows: [][]any{
		{"", "local_view", true, information("VIEW")},
		{"global_temp", "global_view", true, nil},
	}}
	require.Equal(t, map[string]string{
		"local_view":  "VIEW",
		"global_view": "VIEW",
	}, tableTypes(t, client, nil))
}

func TestGetTablesMissingType(t *testing.T) {
	noInfo := arrow.NewSchema([]arrow.Field{
		{Name: "namespace", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "tableName", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "isTemporary", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	for name, tc := range map[string]struct {
		schema *arrow.Schema
		row    []any
	}{
		"no Type line":          {listSchema, []any{"default", "t", false, "Catalog: spark_catalog\nTable: t\n"}},
		"empty Type value":      {listSchema, []any{"default", "t", false, "Type: \n"}},
		"null information":      {listSchema, []any{"default", "t", false, nil}},
		"no information column": {noInfo, []any{"default", "t", false}},
	} {
		t.Run(name, func(t *testing.T) {
			client := &catalogClient{listSchema: tc.schema, listRows: [][]any{tc.row}}
			require.Equal(t, map[string]string{"t": TableTypeTable}, tableTypes(t, client, nil))
		})
	}
}

func TestGetTablesNameFilters(t *testing.T) {
	rows := [][]any{
		{"default", "session_a", false, information("MANAGED")},
		{"", "session_b", true, information("VIEW")},
		{"default", "other", false, information("MANAGED")},
		{"default", "a.b", false, information("MANAGED")},
		{"default", "axb", false, information("MANAGED")},
	}

	// A translatable pattern goes to SHOW TABLE EXTENDED.
	client := &catalogClient{listSchema: listSchema, listRows: rows}
	filter := "session%"
	require.Equal(t, map[string]string{
		"session_a": "MANAGED",
		"session_b": "VIEW",
	}, tableTypes(t, client, &filter))
	require.Equal(t, "SHOW TABLE EXTENDED IN `spark_catalog`.`default` LIKE 'session*'", client.queries[0])

	// A pattern that can't be translated lists everything, and the names are
	// matched on the client.
	client = &catalogClient{listSchema: listSchema, listRows: rows}
	filter = "a.b"
	require.Equal(t, map[string]string{"a.b": "MANAGED"}, tableTypes(t, client, &filter))
	require.Equal(t, "SHOW TABLE EXTENDED IN `spark_catalog`.`default` LIKE '*'", client.queries[0])
}

func TestGetTablesColumnsDescribeTemporaryViewsByName(t *testing.T) {
	client := &catalogClient{
		listSchema: listSchema,
		listRows: [][]any{
			{"default", "t", false, information("MANAGED")},
			{"", "local_view", true, information("VIEW")},
			{"global_temp", "global_view", true, nil},
			{"default", "gone", false, information("MANAGED")},
		},
		describe: map[string][][]string{
			"`spark_catalog`.`default`.`t`": {{"id", "int"}},
			"`local_view`":                  {{"c", "string"}},
			"`global_temp`.`global_view`":   {{"x", "bigint"}},
		},
	}
	tables, err := DefaultGetTablesForDBSchemaImpl(client, context.Background(), "spark_catalog", "default", nil, nil, true)
	require.NoError(t, err)

	columns := map[string][]string{}
	types := map[string]string{}
	for _, table := range tables {
		types[table.TableName] = table.TableType
		for _, col := range table.TableColumns {
			columns[table.TableName] = append(columns[table.TableName], col.ColumnName)
		}
	}
	require.Equal(t, map[string][]string{
		"t":           {"id"},
		"local_view":  {"c"},
		"global_view": {"x"},
	}, columns)
	require.Equal(t, "VIEW", types["local_view"])
	// A table that can't be described is listed without columns.
	require.Contains(t, types, "gone")
	require.Empty(t, columns["gone"])
	require.Contains(t, client.queries, "DESCRIBE TABLE `local_view`")
	require.Contains(t, client.queries, "DESCRIBE TABLE `global_temp`.`global_view`")
}

func TestGetTablesRequiresNameColumn(t *testing.T) {
	bad := arrow.NewSchema([]arrow.Field{{Name: "namespace", Type: arrow.BinaryTypes.String}}, nil)
	client := &catalogClient{listSchema: bad}
	_, err := DefaultGetTablesForDBSchemaImpl(client, context.Background(), "spark_catalog", "default", nil, nil, false)
	require.Error(t, err)
}

func TestGetTablesFallsBackToShowTablesForV2Catalog(t *testing.T) {
	client := &catalogClient{
		listSchema:  listSchema,
		extendedErr: v2Error,
		tablesRows: [][]any{
			{"default", "iceberg_t", false},
			{"", "tmp", true},
		},
	}
	require.Equal(t, map[string]string{
		"iceberg_t": TableTypeTable,
		"tmp":       TableTypeView,
	}, tableTypes(t, client, nil))
	require.Equal(t, []string{
		"SHOW TABLE EXTENDED IN `spark_catalog`.`default` LIKE '*'",
		"SHOW TABLES IN `spark_catalog`.`default` LIKE '*'",
	}, client.queries)
}

func TestGetTablesFallbackKeepsNameFilter(t *testing.T) {
	client := &catalogClient{
		listSchema:  listSchema,
		extendedErr: v2Error,
		tablesRows: [][]any{
			{"default", "iceberg_t", false},
			{"default", "other", false},
		},
	}
	filter := "iceberg%"
	require.Equal(t, map[string]string{"iceberg_t": TableTypeTable}, tableTypes(t, client, &filter))
	require.Equal(t, "SHOW TABLES IN `spark_catalog`.`default` LIKE 'iceberg*'", client.queries[1])
}

func TestGetTablesDoesNotFallBackOnOtherErrors(t *testing.T) {
	client := &catalogClient{
		listSchema: listSchema,
		extendedErr: adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  "[spark] Could not execute query: [SCHEMA_NOT_FOUND] The schema `spark_catalog`.`missing` cannot be found.",
		},
		tablesRows: [][]any{{"default", "t", false}},
	}
	_, err := DefaultGetTablesForDBSchemaImpl(client, context.Background(), "spark_catalog", "default", nil, nil, false)
	require.Error(t, err)
	require.Len(t, client.queries, 1, "an unrelated error must not run SHOW TABLES")
}
