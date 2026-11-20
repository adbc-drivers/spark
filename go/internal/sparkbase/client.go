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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	connectpb "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type QueryContext struct {
	ReaderOptions driverbase.BaseRecordReaderOptions
	Mem           memory.Allocator
	Log           *slog.Logger
	Query         string
	// PosArgs binds the '?' markers of Query. A backend that can't bind them
	// must return an error, not run the query without them. See RejectPosArgs.
	PosArgs []*connectpb.Expression_Literal
}

// RejectPosArgs returns an error if the query has bound parameters, for a
// backend that can't bind them.
func RejectPosArgs(backend string, q QueryContext) error {
	if len(q.PosArgs) == 0 {
		return nil
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("[spark] query parameters are only supported with Spark Connect, not %s", backend),
	}
}

type SparkClient interface {
	driverbase.DbObjectsEnumerator

	BackendName() string
	Close(ctx context.Context) error

	ExecuteQuery(ctx context.Context, query QueryContext) (array.RecordReader, int64, error)
	ExecuteUpdate(ctx context.Context, query QueryContext) (int64, error)
	ExecuteLogicalPlan(ctx context.Context, query QueryContext, plan *connectpb.Plan) (array.RecordReader, error)

	CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error)
	SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error

	CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error)
	SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error

	VendorVersion(ctx context.Context, mem memory.Allocator) (string, error)

	GetOption(ctx context.Context, key string) (string, bool, error)
	GetOptionInt(ctx context.Context, key string) (int64, bool, error)
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
	sql := fmt.Sprintf("USE %s", QuoteIdentifier(catalog))
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
	sql := fmt.Sprintf("USE SCHEMA %s", QuoteIdentifier(schema))
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

// NamePattern compiles an ADBC name pattern (SQL LIKE syntax) to a regexp
// that matches names on the client, ignoring case. A nil or empty pattern
// gives nil, which MatchesName takes to match every name.
func NamePattern(pattern *string) (*regexp.Regexp, error) {
	if pattern == nil || *pattern == "" {
		return nil, nil
	}
	return driverbase.PatternToRegexp(pattern)
}

// MatchesName reports whether name matches a regexp from NamePattern.
func MatchesName(match *regexp.Regexp, name string) bool {
	return match == nil || match.MatchString(name)
}

// nameFilter turns an ADBC name pattern into a LIKE clause for Spark's SHOW
// commands, and a regexp from NamePattern that matches names on the client.
//
// SHOW ... LIKE takes Spark's own syntax: a regex in which '*' matches any
// characters and '|' separates alternatives, matched ignoring the case of
// ASCII letters only. So only a pattern of ASCII letters, digits, '%', and
// '_' gets a LIKE clause, which needs no quoting; any other pattern is
// matched only on the client. Names in Spark's session catalog can only
// contain those characters anyway.
func nameFilter(pattern *string) (string, *regexp.Regexp, error) {
	match, err := NamePattern(pattern)
	if err != nil || match == nil {
		return "", nil, err
	}
	var like strings.Builder
	for _, r := range *pattern {
		switch {
		case r == '%':
			like.WriteByte('*')
		case r == '_':
			like.WriteByte('.')
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
			like.WriteRune(r)
		default:
			return "", match, nil
		}
	}
	return " LIKE " + QuoteString(like.String()), match, nil
}

func DefaultGetCatalogsImpl(c SparkClient, ctx context.Context, catalogFilter *string) ([]string, error) {
	like, pattern, err := nameFilter(catalogFilter)
	if err != nil {
		return nil, err
	}

	rr, err := executeMetadataQuery(c, ctx, "SHOW CATALOGS"+like)
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
			if name := col.Value(i); MatchesName(pattern, name) {
				catalogs = append(catalogs, strings.Clone(name))
			}
		}
	}
	return catalogs, nil
}

func DefaultGetDBSchemasForCatalogImpl(c SparkClient, ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	like, pattern, err := nameFilter(schemaFilter)
	if err != nil {
		return nil, err
	}

	rr, err := executeMetadataQuery(c, ctx, "SHOW SCHEMAS IN "+QuoteIdentifier(catalog)+like)
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
			if name := col.Value(i); MatchesName(pattern, name) {
				schemas = append(schemas, strings.Clone(name))
			}
		}
	}
	return schemas, nil
}

// TableTypeTable is the type of a table whose SHOW TABLE EXTENDED information
// has no Type: line. Every other table type comes from that line.
const TableTypeTable = "TABLE"

// TableTypeView is the type of a view and of a temporary view.
const TableTypeView = "VIEW"

func DefaultGetTablesForDBSchemaImpl(c SparkClient, ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	tables, err := listTablesExtended(c, ctx, catalog, schema, tableFilter)
	if err != nil {
		return nil, err
	}
	if includeColumns {
		return defaultGetTablesWithColumns(c, ctx, catalog, schema, tables, columnFilter)
	}
	infos := make([]driverbase.TableInfo, 0, len(tables))
	for _, table := range tables {
		infos = append(infos, driverbase.TableInfo{
			TableName:    table.name,
			TableType:    table.tableType,
			TableColumns: []driverbase.ColumnInfo{},
		})
	}
	return infos, nil
}

// listedTable is a row of SHOW TABLE EXTENDED that matches the name filter.
type listedTable struct {
	// namespace is the database of a temporary view: "" for a local view, and
	// "global_temp" for a global one. It's empty for other tables.
	namespace string
	name      string
	tableType string
	temporary bool
}

// listTablesExtended lists the tables of a schema with SHOW TABLE EXTENDED,
// which reads each table's metadata to get its type. Temporary views are
// listed too, in every schema.
func listTablesExtended(c SparkClient, ctx context.Context, catalog string, schema string, tableFilter *string) ([]listedTable, error) {
	like, pattern, err := nameFilter(tableFilter)
	if err != nil {
		return nil, err
	}
	// SHOW TABLE EXTENDED requires a LIKE clause. A pattern that can't be
	// translated lists everything, and the names are matched on the client.
	if like == "" {
		like = " LIKE " + QuoteString("*")
	}

	query := "SHOW TABLE EXTENDED IN " + QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + like
	tables, err := listTablesByQuery(c, ctx, query, pattern)
	if isV2ShowTableExtendedError(err) {
		// Spark 3.5 rejects SHOW TABLE EXTENDED on a V2 catalog, such as an
		// Iceberg catalog. SHOW TABLES still lists it. Its rows have no Type:
		// line, so its tables are TABLE, and temporary views are VIEW.
		query = "SHOW TABLES IN " + QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + like
		return listTablesByQuery(c, ctx, query, pattern)
	}
	return tables, err
}

// v2ShowTableExtendedMsg is the message of the error Spark 3.5 raises for SHOW
// TABLE EXTENDED on a V2 table (_LEGACY_ERROR_TEMP_1200, "<name> is not
// supported for v2 tables.").
const v2ShowTableExtendedMsg = "SHOW TABLE EXTENDED is not supported for v2 tables"

func isV2ShowTableExtendedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), v2ShowTableExtendedMsg)
}

// listTablesByQuery runs a SHOW TABLES or SHOW TABLE EXTENDED query and returns
// the rows whose names match pattern.
func listTablesByQuery(c SparkClient, ctx context.Context, query string, pattern *regexp.Regexp) ([]listedTable, error) {
	rr, err := executeMetadataQuery(c, ctx, query)
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	namespaceIdx, nameIdx, temporaryIdx, informationIdx := extendedColumns(rr.Schema())
	if nameIdx < 0 {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[spark] SHOW TABLE EXTENDED did not return expected columns",
		}
	}

	tables := make([]listedTable, 0)
	for rr.Next() {
		rec := rr.RecordBatch()
		nameCol, ok := rec.Column(nameIdx).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] SHOW TABLE EXTENDED did not return expected columns",
			}
		}
		namespaceCol := stringColumn(rec, namespaceIdx)
		informationCol := stringColumn(rec, informationIdx)
		var temporary *array.Boolean
		if temporaryIdx >= 0 {
			temporary, _ = rec.Column(temporaryIdx).(*array.Boolean)
		}
		for i := range int(rec.NumRows()) {
			name := nameCol.Value(i)
			if !MatchesName(pattern, name) {
				continue
			}
			table := listedTable{
				name:      strings.Clone(name),
				temporary: temporary != nil && !temporary.IsNull(i) && temporary.Value(i),
			}
			if namespaceCol != nil && !namespaceCol.IsNull(i) {
				table.namespace = strings.Clone(namespaceCol.Value(i))
			}
			information := ""
			if informationCol != nil && !informationCol.IsNull(i) {
				information = informationCol.Value(i)
			}
			table.tableType = tableTypeOf(information, table.temporary)
			tables = append(tables, table)
		}
	}
	if err := rr.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

// extendedColumns finds the columns of a SHOW TABLE EXTENDED result by name. A
// missing column has index -1.
func extendedColumns(schema *arrow.Schema) (namespace, name, temporary, information int) {
	namespace, name, temporary, information = -1, -1, -1, -1
	if schema == nil {
		return
	}
	for i, field := range schema.Fields() {
		switch {
		case strings.EqualFold(field.Name, "namespace"):
			namespace = i
		case strings.EqualFold(field.Name, "tableName"):
			name = i
		case strings.EqualFold(field.Name, "isTemporary"):
			temporary = i
		case strings.EqualFold(field.Name, "information"):
			information = i
		}
	}
	return
}

// stringColumn returns column idx of rec as a string column, or nil if idx is
// -1 or the column isn't a string.
func stringColumn(rec arrow.RecordBatch, idx int) array.StringLike {
	if idx < 0 {
		return nil
	}
	col, _ := rec.Column(idx).(array.StringLike)
	return col
}

// tableTypeOf returns the ADBC type of a table from the Type: line of its
// SHOW TABLE EXTENDED information. A temporary view is a VIEW. A table with
// no Type: line is a TABLE.
func tableTypeOf(information string, temporary bool) string {
	if temporary {
		return TableTypeView
	}
	for line := range strings.SplitSeq(information, "\n") {
		if value, ok := strings.CutPrefix(line, "Type: "); ok {
			if value = strings.TrimSpace(value); value != "" {
				return value
			}
		}
	}
	return TableTypeTable
}

func defaultGetTablesWithColumns(c SparkClient, ctx context.Context, catalog string, schema string, tables []listedTable, columnFilter *string) ([]driverbase.TableInfo, error) {
	columnFilterRe, err := driverbase.PatternToRegexp(columnFilter)
	if err != nil {
		return nil, err
	}

	infos := make([]driverbase.TableInfo, 0, len(tables))
	for _, table := range tables {
		info := driverbase.TableInfo{
			TableName:    table.name,
			TableType:    table.tableType,
			TableColumns: []driverbase.ColumnInfo{},
		}
		// A temporary view isn't in the schema's catalog, so it's described by
		// its unqualified name, or global_temp.name for a global view.
		ref := qualifiedTable(catalog, schema, table.name)
		if table.temporary {
			ref = temporaryViewRef(table.namespace, table.name)
		}
		columns, err := describeRelation(c, ctx, ref)
		if err != nil {
			// A table that's missing or can't be read shouldn't fail the
			// whole listing; list it without columns. Other errors still
			// fail the call.
			adbcErr, ok := errors.AsType[adbc.Error](err)
			if ok && (adbcErr.Code == adbc.StatusNotFound || adbcErr.Code == adbc.StatusUnauthorized) {
				infos = append(infos, info)
				continue
			}
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
		info.TableColumns = columns
		infos = append(infos, info)
	}
	return infos, nil
}

func qualifiedTable(catalog, schema, table string) string {
	return QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}

func temporaryViewRef(namespace, view string) string {
	if namespace == "" {
		return QuoteIdentifier(view)
	}
	return QuoteIdentifier(namespace) + "." + QuoteIdentifier(view)
}

// describeRelation lists the columns of a table or view. ref is its name, as
// DESCRIBE TABLE takes it.
func describeRelation(c SparkClient, ctx context.Context, ref string) ([]driverbase.ColumnInfo, error) {
	rr, err := executeMetadataQuery(c, ctx, "DESCRIBE TABLE "+ref)
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
