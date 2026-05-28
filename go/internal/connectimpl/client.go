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

package connectimpl

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/adbc-drivers/apache/go/internal/sparkbase"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sparksql "github.com/apache/spark-connect-go/v40/spark/sql"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

type AuthType uint8

const (
	AuthTypeNone AuthType = iota
	AuthTypeToken
)

type ConnectionOpts struct {
	// Host is "hostname" or "hostname:port".
	Host string

	AuthType AuthType
	Username string
	// Token is the OAuth2 bearer token used when AuthType is AuthTypeToken.
	// The spark-connect-go client enables TLS when a token is present.
	Token string
}

type connectClient struct {
	session sparksql.SparkSession
}

// NewClient creates a SparkClient backed by a Spark Connect gRPC session.
func NewClient(ctx context.Context, opts ConnectionOpts, sessionConfig map[string]string) (sparkbase.SparkClient, error) {
	connStr := buildConnectionString(opts)

	session, err := sparksql.NewSessionBuilder().Remote(connStr).Build(ctx)
	if err != nil {
		return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "build Spark Connect session")
	}

	cfg := session.Config()
	for k, v := range sessionConfig {
		if err := cfg.Set(ctx, k, v); err != nil {
			_ = session.Stop()
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "set Spark config %s", k)
		}
	}

	return &connectClient{session: session}, nil
}

func buildConnectionString(opts ConnectionOpts) string {
	var b strings.Builder
	b.WriteString("sc://")
	b.WriteString(opts.Host)
	b.WriteString("/")
	if opts.Token != "" {
		fmt.Fprintf(&b, ";token=%s", url.QueryEscape(opts.Token))
	}
	if opts.Username != "" {
		fmt.Fprintf(&b, ";user_id=%s", url.QueryEscape(opts.Username))
	}
	return b.String()
}

func (c *connectClient) Close() error {
	if c.session == nil {
		return nil
	}
	err := c.session.Stop()
	c.session = nil
	if err != nil {
		return sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "close Spark Connect session")
	}
	return nil
}

func (c *connectClient) ExecuteQuery(ctx context.Context, q sparkbase.QueryContext) (array.RecordReader, int64, error) {
	df, err := c.session.Sql(ctx, q.Query)
	if err != nil {
		return nil, -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute query")
	}

	tbl, err := df.ToArrow(ctx)
	if err != nil {
		return nil, -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "collect Arrow result")
	}

	rr := array.NewTableReader(*tbl, (*tbl).NumRows())
	return rr, (*tbl).NumRows(), nil
}

func (c *connectClient) ExecuteUpdate(ctx context.Context, q sparkbase.QueryContext) (int64, error) {
	// Sql() runs commands eagerly against the Spark Connect server, so the
	// returned DataFrame can be discarded for pure-command queries (DDL, INSERT,
	// UPDATE, DELETE). Spark Connect does not surface a modified-row count.
	if _, err := c.session.Sql(ctx, q.Query); err != nil {
		return -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute update")
	}
	return -1, nil
}

func (c *connectClient) VendorVersion(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultVendorVersionImpl(c, ctx, mem)
}

func (c *connectClient) CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentCatalogImpl(c, ctx, mem)
}

func (c *connectClient) SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error {
	return sparkbase.DefaultSetCurrentCatalogImpl(c, ctx, mem, catalog)
}

func (c *connectClient) CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentSchemaImpl(c, ctx, mem)
}

func (c *connectClient) SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error {
	return sparkbase.DefaultSetCurrentSchemaImpl(c, ctx, mem, schema)
}

func (c *connectClient) executeMetadataQuery(ctx context.Context, sql string) (array.RecordReader, error) {
	query := sparkbase.QueryContext{
		Query: sql,
		Log:   discardLogger,
	}
	rr, _, err := c.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return rr, nil
}

func (c *connectClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW CATALOGS")
	if catalogFilter != nil && *catalogFilter != "" {
		query.WriteString(" LIKE ")
		query.WriteString(sparkbase.QuoteString(*catalogFilter))
	}

	rr, err := c.executeMetadataQuery(ctx, query.String())
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
			catalogs = append(catalogs, string(col.Value(i)))
		}
	}
	return catalogs, nil
}

func (c *connectClient) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW SCHEMAS IN ")
	query.WriteString(sparkbase.QuoteIdentifier(catalog))
	if schemaFilter != nil && *schemaFilter != "" {
		query.WriteString(" LIKE ")
		query.WriteString(sparkbase.QuoteString(*schemaFilter))
	}

	rr, err := c.executeMetadataQuery(ctx, query.String())
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
			schemas = append(schemas, string(col.Value(i)))
		}
	}
	return schemas, nil
}

func (c *connectClient) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	if includeColumns {
		return c.getTablesWithColumns(ctx, catalog, schema, tableFilter, columnFilter)
	}
	return c.getTablesOnly(ctx, catalog, schema, tableFilter)
}

func (c *connectClient) getTablesOnly(ctx context.Context, catalog string, schema string, tableFilter *string) ([]driverbase.TableInfo, error) {
	var query strings.Builder
	query.WriteString("SHOW TABLES IN ")
	query.WriteString(sparkbase.QuoteIdentifier(catalog))
	query.WriteString(".")
	query.WriteString(sparkbase.QuoteIdentifier(schema))
	if tableFilter != nil && *tableFilter != "" {
		query.WriteString(" LIKE ")
		query.WriteString(sparkbase.QuoteString(*tableFilter))
	}

	rr, err := c.executeMetadataQuery(ctx, query.String())
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	tables := make([]driverbase.TableInfo, 0)
	for rr.Next() {
		rec := rr.RecordBatch()
		// SHOW TABLES returns: namespace, tableName, isTemporary
		nameCol, ok := rec.Column(1).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] SHOW TABLES did not return expected columns",
			}
		}
		for i := range int(rec.NumRows()) {
			tableName := string(nameCol.Value(i))
			tables = append(tables, driverbase.TableInfo{
				TableName:    tableName,
				TableType:    "TABLE",
				TableColumns: []driverbase.ColumnInfo{},
			})
		}
	}
	return tables, nil
}

func (c *connectClient) getTablesWithColumns(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string) ([]driverbase.TableInfo, error) {
	tables, err := c.getTablesOnly(ctx, catalog, schema, tableFilter)
	if err != nil {
		return nil, err
	}

	columnFilterRe, err := driverbase.PatternToRegexp(columnFilter)
	if err != nil {
		return nil, err
	}

	for idx := range tables {
		columns, err := c.describeTable(ctx, catalog, schema, tables[idx].TableName)
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

func (c *connectClient) describeTable(ctx context.Context, catalog string, schema string, table string) ([]driverbase.ColumnInfo, error) {
	sql := fmt.Sprintf("DESCRIBE TABLE %s.%s.%s",
		sparkbase.QuoteIdentifier(catalog),
		sparkbase.QuoteIdentifier(schema),
		sparkbase.QuoteIdentifier(table))

	rr, err := c.executeMetadataQuery(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	var columns []driverbase.ColumnInfo
	var ordinal int32
	for rr.Next() {
		rec := rr.RecordBatch()
		// DESCRIBE TABLE returns: col_name, data_type, comment
		colNameCol, ok := rec.Column(0).(array.StringLike)
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "[spark] DESCRIBE TABLE did not return expected columns",
			}
		}
		dataTypeCol, _ := rec.Column(1).(array.StringLike)

		for i := range int(rec.NumRows()) {
			colName := string(colNameCol.Value(i))
			// DESCRIBE TABLE may include partition info or empty separator rows
			if colName == "" || colName[0] == '#' {
				break
			}

			ordinal++
			col := driverbase.ColumnInfo{
				ColumnName:      colName,
				OrdinalPosition: new(ordinal),
				XdbcTypeName:    new(string(dataTypeCol.Value(i))),
				XdbcNullable:    new(int16(1)),
				XdbcIsNullable:  new("YES"),
			}
			columns = append(columns, col)
		}
	}
	return columns, nil
}

var _ sparkbase.SparkClient = (*connectClient)(nil)
