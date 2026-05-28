// Copyright (c) 2025 ADBC Drivers Contributors
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
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type QueryContext struct {
	ReaderOptions driverbase.BaseRecordReaderOptions
	Mem           memory.Allocator
	Log           *slog.Logger
	Query         string
}

type SparkClient interface {
	io.Closer
	driverbase.DbObjectsEnumerator

	ExecuteQuery(ctx context.Context, query QueryContext) (array.RecordReader, int64, error)
	ExecuteUpdate(ctx context.Context, query QueryContext) (int64, error)

	CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error)
	SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error

	CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error)
	SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error

	VendorVersion(ctx context.Context, mem memory.Allocator) (string, error)
}

type SparkClientFactory func(context.Context) (SparkClient, error)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func singleRowStringQuery(sql string, c SparkClient, ctx context.Context, mem memory.Allocator) (string, error) {
	query := QueryContext{
		Query: sql,
		Mem:   mem,
		Log:   discardLogger,
	}
	rr, _, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return "", err
	}
	defer rr.Release()

	if !rr.Next() {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return any rows", query.Query),
		}
	}

	rec := rr.RecordBatch()
	if rec.NumRows() != 1 {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a single row", query.Query),
		}
	}

	if stringCol, ok := rec.Column(0).(array.StringLike); ok {
		// force copy as arrow-go by default slices the internal allocation
		return strings.Clone(stringCol.Value(0)), nil
	}

	return "", adbc.Error{
		Code: adbc.StatusInternal,
		Msg:  fmt.Sprintf("[spark] `%s` did not return a string result", query.Query),
	}
}

// The following are blanket implementations of metadata queries based on information schema queries.
// Concrete APIs may offer different implementations if the underlying API has a faster way to pull
// that data.

func DefaultVendorVersionImpl(c SparkClient, ctx context.Context, mem memory.Allocator) (string, error) {
	return singleRowStringQuery("SELECT version()", c, ctx, mem)
}

func DefaultCurrentCatalogImpl(c SparkClient, ctx context.Context, mem memory.Allocator) (string, error) {
	return singleRowStringQuery("SELECT current_catalog()", c, ctx, mem)
}

func DefaultCurrentSchemaImpl(c SparkClient, ctx context.Context, mem memory.Allocator) (string, error) {
	return singleRowStringQuery("SELECT current_schema()", c, ctx, mem)
}

func DefaultSetCurrentCatalogImpl(c SparkClient, ctx context.Context, mem memory.Allocator, catalog string) error {
	sql := fmt.Sprintf("USE CATALOG %s", QuoteString(catalog))
	query := QueryContext{
		Query: sql,
		Mem:   mem,
	}
	_, err := c.ExecuteUpdate(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func DefaultSetCurrentSchemaImpl(c SparkClient, ctx context.Context, mem memory.Allocator, schema string) error {
	sql := fmt.Sprintf("USE SCHEMA %s", QuoteString(schema))
	query := QueryContext{
		Query: sql,
		Mem:   mem,
	}
	_, err := c.ExecuteUpdate(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func executeMetadataQuery(c SparkClient, ctx context.Context, sql string) (array.RecordReader, error) {
	query := QueryContext{
		Query: sql,
		Mem:   memory.DefaultAllocator,
		Log:   discardLogger,
	}
	rr, _, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return rr, nil
}

func DefaultGetCatalogsImpl(c SparkClient, ctx context.Context, catalogFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW CATALOGS")
	if catalogFilter != nil && *catalogFilter != "" {
		query.WriteString(" LIKE ")
		query.WriteString(QuoteString(*catalogFilter))
	}

	rr, err := executeMetadataQuery(c, ctx, query.String())
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	var catalogs []string
	for rr.Next() {
		rec := rr.RecordBatch()
		col, ok := rec.Column(0).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] SHOW CATALOGS did not return a string column",
			}
		}
		for i := range int(rec.NumRows()) {
			catalogs = append(catalogs, strings.Clone(col.Value(i)))
		}
	}
	return catalogs, nil
}

func DefaultGetDBSchemasForCatalogImpl(c SparkClient, ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW SCHEMAS IN ")
	query.WriteString(QuoteIdentifier(catalog))
	if schemaFilter != nil && *schemaFilter != "" {
		query.WriteString(" LIKE ")
		query.WriteString(QuoteString(*schemaFilter))
	}

	rr, err := executeMetadataQuery(c, ctx, query.String())
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	var schemas []string
	for rr.Next() {
		rec := rr.RecordBatch()
		col, ok := rec.Column(0).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] SHOW SCHEMAS did not return a string column",
			}
		}
		for i := range int(rec.NumRows()) {
			schemas = append(schemas, strings.Clone(col.Value(i)))
		}
	}
	return schemas, nil
}

func DefaultGetTablesForDBSchemaImpl(c SparkClient, ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	if includeColumns {
		return defaultGetTablesWithColumns(c, ctx, catalog, schema, tableFilter, columnFilter)
	}
	return defaultGetTablesOnly(c, ctx, catalog, schema, tableFilter)
}

func defaultGetTablesOnly(c SparkClient, ctx context.Context, catalog string, schema string, tableFilter *string) ([]driverbase.TableInfo, error) {
	var query strings.Builder
	query.WriteString("SHOW TABLES IN ")
	query.WriteString(QuoteIdentifier(catalog))
	query.WriteString(".")
	query.WriteString(QuoteIdentifier(schema))
	if tableFilter != nil && *tableFilter != "" {
		query.WriteString(" LIKE ")
		query.WriteString(QuoteString(*tableFilter))
	}

	rr, err := executeMetadataQuery(c, ctx, query.String())
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	tables := make([]driverbase.TableInfo, 0)
	for rr.Next() {
		rec := rr.RecordBatch()
		nameCol, ok := rec.Column(1).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] SHOW TABLES did not return expected columns",
			}
		}
		for i := range int(rec.NumRows()) {
			tableName := strings.Clone(nameCol.Value(i))
			tables = append(tables, driverbase.TableInfo{
				TableName:    tableName,
				TableType:    "TABLE",
				TableColumns: []driverbase.ColumnInfo{},
			})
		}
	}
	return tables, nil
}

func defaultGetTablesWithColumns(c SparkClient, ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string) ([]driverbase.TableInfo, error) {
	tables, err := defaultGetTablesOnly(c, ctx, catalog, schema, tableFilter)
	if err != nil {
		return nil, err
	}

	columnFilterRe, err := driverbase.PatternToRegexp(columnFilter)
	if err != nil {
		return nil, err
	}

	for idx := range tables {
		columns, err := defaultDescribeTable(c, ctx, catalog, schema, tables[idx].TableName)
		if err != nil {
			return nil, err
		}
		if columnFilterRe != nil {
			filtered := make([]driverbase.ColumnInfo, 0, len(columns))
			for _, col := range columns {
				if columnFilterRe.MatchString(col.ColumnName) {
					filtered = append(filtered, col)
				}
			}
			columns = filtered
		}
		tables[idx].TableColumns = columns
	}
	return tables, nil
}

func defaultDescribeTable(c SparkClient, ctx context.Context, catalog string, schema string, table string) ([]driverbase.ColumnInfo, error) {
	sql := fmt.Sprintf("DESCRIBE TABLE %s.%s.%s",
		QuoteIdentifier(catalog),
		QuoteIdentifier(schema),
		QuoteIdentifier(table))

	rr, err := executeMetadataQuery(c, ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	var columns []driverbase.ColumnInfo
	var ordinal int32
	for rr.Next() {
		rec := rr.RecordBatch()
		colNameCol, ok := rec.Column(0).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] DESCRIBE TABLE did not return expected columns",
			}
		}
		dataTypeCol, _ := rec.Column(1).(array.StringLike)

		for i := range int(rec.NumRows()) {
			colName := strings.Clone(colNameCol.Value(i))
			if colName == "" || colName[0] == '#' {
				break
			}

			ordinal++
			col := driverbase.ColumnInfo{
				ColumnName:      colName,
				OrdinalPosition: new(ordinal),
				XdbcTypeName:    new(strings.Clone(dataTypeCol.Value(i))),
				XdbcNullable:    new(int16(1)),
				XdbcIsNullable:  new("YES"),
			}
			columns = append(columns, col)
		}
	}
	return columns, nil
}
