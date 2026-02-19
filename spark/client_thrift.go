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

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/apache/spark/internal/hiveserver2"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/columnar-tech/sasl-go"
)

type transportType int

const (
	http transportType = iota
	binary
)

type authType int

const (
	noSasl authType = iota
	plain
)

type thriftConnectionOpts struct {
	transport transportType
	auth      authType

	catalog string

	username string
	password string

	host string
}

type thriftClient struct {
	transport thrift.TTransport
	// TODO(lidavidm): do we need the lock if we're using the HTTP client?
	client  *driverbase.Shared[hiveserver2.TCLIServiceClient]
	session *hiveserver2.TSessionHandle
}

type nilCloser struct{}

func (nilCloser) Close() error { return nil }

func wrapThriftTransport(ctx context.Context, cfg *thrift.TConfiguration, transport thrift.TTransport) (sparkClient, error) {
	factory := thrift.NewTBinaryProtocolFactoryConf(cfg)
	iprot := factory.GetProtocol(transport)
	oprot := factory.GetProtocol(transport)

	client := hiveserver2.NewTCLIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	req := &hiveserver2.TOpenSessionReq{}
	resp, err := client.OpenSession(ctx, req)
	if err = toAdbcErr(adbc.StatusIO, err, resp, "open HiveServer2 session"); err != nil {
		return nil, errors.Join(err, transport.Close())
	}
	return &thriftClient{
		transport: transport,
		client:    driverbase.NewShared(client, nilCloser{}),
		session:   resp.SessionHandle,
	}, nil
}

func newThriftClient(ctx context.Context, opts thriftConnectionOpts) (sparkClient, error) {
	var (
		transport thrift.TTransport
		err       error
	)
	cfg := &thrift.TConfiguration{}

	switch opts.transport {
	case http:
		uri := fmt.Sprintf("http://%s", opts.host)
		transport, err = thrift.NewTHttpClient(uri)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err, "could not open HTTP thrift client")
		}

		switch opts.auth {
		case noSasl:
			// It seems Spark expects the header but does not do anything with it
			transport.(*thrift.THttpClient).SetHeader("Authorization", "Basic DummyToken")
		case plain:
			transport.(*thrift.THttpClient).SetHeader("Authorization", fmt.Sprintf("Basic %s:%s", opts.username, opts.password))
		}
	case binary:
		transport = thrift.NewTSocketConf(opts.host, cfg)
		if err := transport.Open(); err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err, "could not open binary thrift client")
		}

		switch opts.auth {
		case noSasl:
		case plain:
			// It seems Spark expects the password to be non-empty
			password := opts.password
			if password == "" {
				password = "x"
			}

			transport = sasl.WrapTransport(transport, opts.host, &sasl.PlainMechanism{
				Username: opts.username,
				Password: password,
			})
		}
	}

	return wrapThriftTransport(ctx, cfg, transport)
}

func (c *thriftClient) Close() error {
	if c.transport == nil {
		return adbc.Error{
			Msg:  "[spark] connection not initialized or already closed",
			Code: adbc.StatusInvalidState,
		}
	}

	// TODO: close session
	c.session = nil
	c.client = nil

	if err := c.transport.Close(); err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "close thrift transport")
	}
	c.transport = nil

	return nil
}

func (c *thriftClient) simpleQuery(ctx context.Context, query string, context string) (*hiveserver2.TFetchResultsResp, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: c.session,
		Statement:     query,
	}
	return driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TFetchResultsResp, error) {
		resp, err := client.ExecuteStatement(ctx, req)
		if err = toAdbcErr(adbc.StatusIO, err, resp, "%s", context); err != nil {
			return nil, err
		}

		if !resp.OperationHandle.HasResultSet {
			return nil, nil
		}

		rs, err := client.FetchResults(ctx, &hiveserver2.TFetchResultsReq{
			OperationHandle: resp.OperationHandle,
			Orientation:     hiveserver2.TFetchOrientation_FETCH_FIRST,
			MaxRows:         1,
		})
		if err = toAdbcErr(adbc.StatusIO, err, resp, "%s", context); err != nil {
			return nil, err
		}

		return rs, nil
	})
}

func (c *thriftClient) currentCatalog(ctx context.Context) (string, error) {
	query := "SELECT current_catalog()"
	rs, err := c.simpleQuery(ctx, query, "get current catalog")
	if err != nil {
		return "", err
	} else if rs == nil {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a result set", query),
		}
	}

	if len(rs.Results.Rows) != 1 {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a single row", query),
		}
	}

	return *rs.Results.Rows[0].ColVals[0].GetStringVal().Value, nil
}

func (c *thriftClient) currentSchema(ctx context.Context) (string, error) {
	query := "SELECT current_schema()"
	rs, err := c.simpleQuery(ctx, query, "get current schema")
	if err != nil {
		return "", err
	} else if rs == nil {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a result set", query),
		}
	}

	if len(rs.Results.Rows) != 1 {
		return "", adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a single row", query),
		}
	}

	return *rs.Results.Rows[0].ColVals[0].GetStringVal().Value, nil
}

func (c *thriftClient) executeQuery(ctx context.Context, query queryContext) (array.RecordReader, int64, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: c.session,
		Statement:     query.query,
	}
	rdr, err := newRecordReader(ctx, query.mem, c.client, req)
	if err != nil {
		return nil, -1, err
	}
	return rdr, -1, nil
}

func (c *thriftClient) executeUpdate(ctx context.Context, query queryContext) (int64, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: c.session,
		Statement:     query.query,
	}
	resp, err := driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TExecuteStatementResp, error) {
		return client.ExecuteStatement(ctx, req)
	})
	if err != nil {
		return -1, errToAdbcErr(adbc.StatusIO, err, "execute statement")
	} else if err = statusToAdbcErr(resp.Status, "execute statement"); err != nil {
		return -1, err
	}
	// TODO: if HasResultSet, do we have to explicitly free it?
	if resp.OperationHandle.ModifiedRowCount == nil {
		return -1, nil
	}
	return int64(*resp.OperationHandle.ModifiedRowCount), nil
}

func (c *thriftClient) setCurrentCatalog(ctx context.Context, catalog string) error {
	query := fmt.Sprintf("USE CATALOG %s", quoteString(catalog))
	_, err := c.simpleQuery(ctx, query, "set current catalog")
	if err != nil {
		return err
	}
	return nil
}

func (c *thriftClient) setCurrentSchema(ctx context.Context, schema string) error {
	query := fmt.Sprintf("USE SCHEMA %s", quoteString(schema))
	_, err := c.simpleQuery(ctx, query, "set current catalog")
	if err != nil {
		return err
	}
	return nil
}

func (c *thriftClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW CATALOGS")
	if catalogFilter != nil && *catalogFilter != "" {
		query.WriteString(" LIKE ")
		// TODO(lidavidm): need to translate the filter to a regex for Spark
		query.WriteString(quoteString(*catalogFilter))
	}
	resp, err := c.simpleQuery(ctx, query.String(), "get catalogs")
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a result set", query.String()),
		}
	}

	catalogs := make([]string, len(resp.Results.Rows))
	for i, row := range resp.Results.Rows {
		catalogs[i] = *row.ColVals[0].GetStringVal().Value
	}
	return catalogs, nil
}

func (c *thriftClient) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW SCHEMAS IN ")
	query.WriteString(quoteIdentifier(catalog))
	if schemaFilter != nil && *schemaFilter != "" {
		query.WriteString(" LIKE ")
		// TODO(lidavidm): need to translate the filter to a regex for Spark
		query.WriteString(quoteString(*schemaFilter))
	}
	resp, err := c.simpleQuery(ctx, query.String(), "get schemas")
	if err != nil {
		return nil, err
	} else if resp == nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("[spark] `%s` did not return a result set", query.String()),
		}
	}

	schemas := make([]string, len(resp.Results.Rows))
	for i, row := range resp.Results.Rows {
		schemas[i] = *row.ColVals[0].GetStringVal().Value
	}
	return schemas, nil
}

func (c *thriftClient) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	// HiveServer2 lacks support for parameters so we have to build the
	// query.  N.B. Databricks extends the protocol with parameters so we
	// should use that when available.
	var tables []driverbase.TableInfo
	if includeColumns {
	} else {
		// TODO: tableFilter
		req := &hiveserver2.TGetTablesReq{
			SessionHandle: c.session,
			CatalogName:   hiveserver2.TPatternOrIdentifierPtr(hiveserver2.TPatternOrIdentifier(catalog)),
			SchemaName:    hiveserver2.TPatternOrIdentifierPtr(hiveserver2.TPatternOrIdentifier(schema)),
		}
		resp, err := driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TFetchResultsResp, error) {
			resp, err := client.GetTables(ctx, req)
			if err = toAdbcErr(adbc.StatusIO, err, resp, "get tables for %s.%s", catalog, schema); err != nil {
				return nil, err
			}

			if !resp.OperationHandle.HasResultSet {
				return nil, adbc.Error{
					Code: adbc.StatusInternal,
					Msg:  "[spark] GetTables did not return a result set",
				}
			}

			// TODO: may need to fetch results multiple times
			rs, err := client.FetchResults(ctx, &hiveserver2.TFetchResultsReq{
				OperationHandle: resp.OperationHandle,
				Orientation:     hiveserver2.TFetchOrientation_FETCH_FIRST,
				MaxRows:         65536,
			})
			if err = toAdbcErr(adbc.StatusIO, err, rs, "get tables for %s.%s", catalog, schema); err != nil {
				return nil, err
			}
			return rs, nil
		})

		if err != nil {
			return nil, err
		}

		// var query strings.Builder
		// query.WriteString("SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = ")
		// query.WriteString(quoteString(catalog))
		// query.WriteString(" AND table_schema = ")
		// query.WriteString(quoteString(schema))

		// resp, err := c.simpleQuery(ctx, query.String(), "get schemas")
		// if err != nil {
		// 	return nil, err
		// } else if resp == nil {
		// 	return nil, adbc.Error{
		// 		Code: adbc.StatusInternal,
		// 		Msg:  fmt.Sprintf("[spark] `%s` did not return a result set", query.String()),
		// 	}
		// }

		for _, row := range resp.Results.Rows {
			tableName := *row.ColVals[2].GetStringVal().Value
			tableType := *row.ColVals[3].GetStringVal().Value
			tables = append(tables, driverbase.TableInfo{
				TableName:        tableName,
				TableType:        tableType,
				TableColumns:     nil,
				TableConstraints: nil,
			})
		}
	}
	return tables, nil
}

var _ sparkClient = (*thriftClient)(nil)
