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
	"net/url"
	"strings"

	"github.com/adbc-drivers/apache/go/internal/sparkbase"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sparksql "github.com/apache/spark-connect-go/v40/spark/sql"
)

type AuthType uint8

const (
	AuthTypeNone AuthType = iota
	AuthTypeToken
)

type ConnectionOpts struct {
	// Host is "hostname" or "hostname:port".
	Host string

	AuthType AuthType // not used
	Username string
	// Token is the OAuth2 bearer token used when AuthType is AuthTypeToken.
	// The spark-connect-go client enables TLS when a token is present.
	Token string
}

type connectClient struct {
	session sparksql.SparkSession
}

func (c *connectClient) BackendName() string {
	return "Spark Connect"
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
	if opts.Token != "" || opts.Username != "" {
		// spark-connect-go rejects trailing slash when there are no other params
		b.WriteString("/")
	}
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

func (c *connectClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	return sparkbase.DefaultGetCatalogsImpl(c, ctx, catalogFilter)
}

func (c *connectClient) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	return sparkbase.DefaultGetDBSchemasForCatalogImpl(c, ctx, catalog, schemaFilter)
}

func (c *connectClient) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	return sparkbase.DefaultGetTablesForDBSchemaImpl(c, ctx, catalog, schema, tableFilter, columnFilter, includeColumns)
}

var _ sparkbase.SparkClient = (*connectClient)(nil)
