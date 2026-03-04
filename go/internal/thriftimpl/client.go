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

package thriftimpl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/apache/go/internal/hiveserver2"
	"github.com/adbc-drivers/apache/go/internal/sparkbase"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/columnar-tech/sasl-go"
)

type Transport int

const (
	Http Transport = iota
	Binary
)

type ThriftAuth int

const (
	NoSasl ThriftAuth = iota
	Plain
)

type ConnectionOpts struct {
	Transport Transport
	Auth      ThriftAuth

	Catalog string

	Username string
	Password string

	Host string
}

type thriftClient struct {
	transport thrift.TTransport
	// TODO(lidavidm): do we need the lock if we're using the HTTP client?
	client  *driverbase.Shared[hiveserver2.TCLIServiceClient]
	session *hiveserver2.TSessionHandle
}

type nilCloser struct{}

func (nilCloser) Close() error { return nil }

func wrapThriftTransport(ctx context.Context, cfg *thrift.TConfiguration, transport thrift.TTransport) (sparkbase.SparkClient, error) {
	factory := thrift.NewTBinaryProtocolFactoryConf(cfg)
	iprot := factory.GetProtocol(transport)
	oprot := factory.GetProtocol(transport)

	client := hiveserver2.NewTCLIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	req := &hiveserver2.TOpenSessionReq{}
	resp, err := client.OpenSession(ctx, req)
	if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "open HiveServer2 session"); err != nil {
		return nil, errors.Join(err, transport.Close())
	}
	return &thriftClient{
		transport: transport,
		client:    driverbase.NewShared(client, nilCloser{}),
		session:   resp.SessionHandle,
	}, nil
}

func NewClient(ctx context.Context, opts ConnectionOpts) (sparkbase.SparkClient, error) {
	var (
		transport thrift.TTransport
		err       error
	)
	cfg := &thrift.TConfiguration{}

	switch opts.Transport {
	case Http:
		uri := opts.Host
		transport, err = thrift.NewTHttpClient(uri)
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "could not open HTTP thrift client")
		}

		switch opts.Auth {
		case NoSasl:
			// It seems Spark expects the header but does not do anything with it
			transport.(*thrift.THttpClient).SetHeader("Authorization", "Basic DummyToken")
		case Plain:
			transport.(*thrift.THttpClient).SetHeader("Authorization", fmt.Sprintf("Basic %s:%s", opts.Username, opts.Password))
		}
	case Binary:
		transport = thrift.NewTSocketConf(opts.Host, cfg)
		if err := transport.Open(); err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "could not open binary thrift client")
		}

		switch opts.Auth {
		case NoSasl:
		case Plain:
			// It seems Spark expects the password to be non-empty
			password := opts.Password
			if password == "" {
				password = "x"
			}

			transport = sasl.WrapTransport(transport, opts.Host, &sasl.PlainMechanism{
				Username: opts.Username,
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
		return sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "close thrift transport")
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
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "%s", context); err != nil {
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
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "%s", context); err != nil {
			return nil, err
		}

		return rs, nil
	})
}

func (c *thriftClient) CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentCatalogImpl(c, ctx, mem)
}

func (c *thriftClient) CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentCatalogImpl(c, ctx, mem)
}

func (c *thriftClient) SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error {
	return sparkbase.DefaultSetCurrentCatalogImpl(c, ctx, mem, catalog)
}

func (c *thriftClient) SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error {
	return sparkbase.DefaultSetCurrentSchemaImpl(c, ctx, mem, schema)
}

func (c *thriftClient) ExecuteQuery(ctx context.Context, query sparkbase.QueryContext) (array.RecordReader, int64, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: c.session,
		Statement:     query.Query,
	}
	rdr, err := newThriftRecordReader(ctx, query.Mem, c.client, req)
	if err != nil {
		return nil, -1, err
	}
	return rdr, -1, nil
}

func (c *thriftClient) ExecuteUpdate(ctx context.Context, query sparkbase.QueryContext) (int64, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: c.session,
		Statement:     query.Query,
	}
	resp, err := driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TExecuteStatementResp, error) {
		return client.ExecuteStatement(ctx, req)
	})
	if err != nil {
		return -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute statement")
	}
	if err = sparkbase.StatusToAdbcErr(resp.Status, "execute statement"); err != nil {
		return -1, err
	}
	// TODO: if HasResultSet, do we have to explicitly free it?
	if resp.OperationHandle.ModifiedRowCount == nil {
		return -1, nil
	}
	return int64(*resp.OperationHandle.ModifiedRowCount), nil
}

func (c *thriftClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	var query strings.Builder
	query.WriteString("SHOW CATALOGS")
	if catalogFilter != nil && *catalogFilter != "" {
		query.WriteString(" LIKE ")
		// TODO(lidavidm): need to translate the filter to a regex for Spark
		query.WriteString(sparkbase.QuoteString(*catalogFilter))
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
	query.WriteString(sparkbase.QuoteIdentifier(catalog))
	if schemaFilter != nil && *schemaFilter != "" {
		query.WriteString(" LIKE ")
		// TODO(lidavidm): need to translate the filter to a regex for Spark
		query.WriteString(sparkbase.QuoteString(*schemaFilter))
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
			if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "get tables for %s.%s", catalog, schema); err != nil {
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
			if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, rs, "get tables for %s.%s", catalog, schema); err != nil {
				return nil, err
			}
			return rs, nil
		})

		if err != nil {
			return nil, err
		}

		// var query strings.Builder
		// query.WriteString("SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = ")
		// query.WriteString(sparkbase.QuoteString(catalog))
		// query.WriteString(" AND table_schema = ")
		// query.WriteString(sparkbase.QuoteString(schema))

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

var _ sparkbase.SparkClient = (*thriftClient)(nil)
