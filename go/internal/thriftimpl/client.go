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
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/spark/go/internal/hiveserver2"
	"github.com/adbc-drivers/spark/go/internal/sasl"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/thrift/lib/go/thrift"
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

	Username                  string
	Password                  string
	Tls                       bool
	ValidateServerCertificate bool

	Host string
}

type thriftClient struct {
	transport     thrift.TTransport
	transportName string
	// TODO(lidavidm): do we need the lock if we're using the HTTP client?
	client  *driverbase.Shared[hiveserver2.TCLIServiceClient]
	session *hiveserver2.TSessionHandle
}

func (c *thriftClient) BackendName() string {
	return fmt.Sprintf("HiveServer2+%s", c.transportName)
}

type nilCloser struct{}

func (nilCloser) Close() error { return nil }

func wrapThriftTransport(ctx context.Context, cfg *thrift.TConfiguration, transport thrift.TTransport, transportName string, sessionOptions map[string]string) (sparkbase.SparkClient, error) {
	factory := thrift.NewTBinaryProtocolFactoryConf(cfg)
	iprot := factory.GetProtocol(transport)
	oprot := factory.GetProtocol(transport)

	client := hiveserver2.NewTCLIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	req := &hiveserver2.TOpenSessionReq{
		Configuration: sessionOptions,
	}
	resp, err := client.OpenSession(ctx, req)
	if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "open HiveServer2 session"); err != nil {
		return nil, errors.Join(err, transport.Close())
	}
	return &thriftClient{
		transport:     transport,
		transportName: transportName,
		client:        driverbase.NewShared(client, nilCloser{}),
		session:       resp.SessionHandle,
	}, nil
}

func NewClient(ctx context.Context, opts ConnectionOpts, sessionOptions map[string]string) (sparkbase.SparkClient, error) {
	var (
		transport thrift.TTransport
		err       error
	)
	cfg := &thrift.TConfiguration{}

	transportName := ""
	switch opts.Transport {
	case Http:
		transportName = "HTTP"
		var uri string
		// TODO: does 'cliservice' path need to be configurable?
		if opts.Tls {
			uri = "https://" + opts.Host + "/cliservice"
		} else {
			uri = "http://" + opts.Host + "/cliservice"
		}
		httpClientOptions := thrift.THttpClientOptions{}
		if !opts.ValidateServerCertificate {
			httpClientOptions.Client = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
			}
		}
		transport, err = thrift.NewTHttpClientWithOptions(uri, httpClientOptions)
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "could not open HTTP thrift client")
		}

		switch opts.Auth {
		case NoSasl:
			// It seems Spark expects the header but does not do anything with it
			transport.(*thrift.THttpClient).SetHeader("Authorization", "Basic DummyToken")
		case Plain:
			creds := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%s:%s", opts.Username, opts.Password))
			transport.(*thrift.THttpClient).SetHeader("Authorization", "Basic "+creds)
		}
	case Binary:
		transportName = "binary"
		transport = thrift.NewTSocketConf(opts.Host, cfg)

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

		if err := transport.Open(); err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "open binary thrift client")
		}

	}

	return wrapThriftTransport(ctx, cfg, transport, transportName, sessionOptions)
}

func (c *thriftClient) Close(_ context.Context) error {
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

func (c *thriftClient) VendorVersion(ctx context.Context, _ memory.Allocator) (string, error) {
	req := &hiveserver2.TGetInfoReq{
		SessionHandle: c.session,
		InfoType:      hiveserver2.TGetInfoType_CLI_DBMS_VER,
	}
	resp, err := driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TGetInfoResp, error) {
		resp, err := client.GetInfo(ctx, req)
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "get vendor version"); err != nil {
			return nil, err
		}
		return resp, nil
	})
	if err != nil {
		return "", err
	}
	if resp.InfoValue != nil && resp.InfoValue.StringValue != nil {
		return *resp.InfoValue.StringValue, nil
	}
	return "", nil
}

func (c *thriftClient) CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentCatalogImpl(c, ctx, mem)
}

func (c *thriftClient) CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentSchemaImpl(c, ctx, mem)
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
	rdr, err := newThriftRecordReader(ctx, query, c.client, req)
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
	if err = sparkbase.StatusToAdbcErr(adbc.StatusIO, resp.Status, "execute statement"); err != nil {
		return -1, err
	}
	// TODO: if HasResultSet, do we have to explicitly free it?
	if resp.OperationHandle.ModifiedRowCount != nil {
		return int64(*resp.OperationHandle.ModifiedRowCount), nil
	}

	statusResp, err := driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TGetOperationStatusResp, error) {
		return client.GetOperationStatus(ctx, &hiveserver2.TGetOperationStatusReq{
			OperationHandle: resp.OperationHandle,
		})
	})
	if err != nil {
		return -1, nil
	}
	if err = sparkbase.StatusToAdbcErr(adbc.StatusIO, statusResp.Status, "get operation status"); err != nil {
		return -1, nil
	}
	if statusResp.NumModifiedRows != nil {
		return *statusResp.NumModifiedRows, nil
	}
	return -1, nil
}

func (c *thriftClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	return sparkbase.DefaultGetCatalogsImpl(c, ctx, catalogFilter)
}

func (c *thriftClient) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	return sparkbase.DefaultGetDBSchemasForCatalogImpl(c, ctx, catalog, schemaFilter)
}

// tablePattern turns an ADBC table name pattern into a pattern for GetTables
// and GetColumns, or nil to list every table. Names are matched on the client
// too, so the pattern only has to match at least the same tables.
//
// Spark's Thrift server converts '%' and '_' to the syntax of SHOW ... LIKE
// and keeps any other character, so it reads '.', '*', and '|' as that syntax
// too, and it ignores the case of ASCII letters only. It also skips the
// second of two '%' or two '_' in a row. So only a pattern of ASCII letters,
// digits, '%', and '_' is sent, with each run of wildcards replaced by '%'.
func tablePattern(pattern *string) *hiveserver2.TPatternOrIdentifier {
	if pattern == nil || *pattern == "" {
		return nil
	}
	sent := make([]byte, 0, len(*pattern))
	for _, r := range *pattern {
		switch {
		case r == '%' || r == '_':
			if n := len(sent); n > 0 && (sent[n-1] == '%' || sent[n-1] == '_') {
				sent[n-1] = '%'
				continue
			}
		case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
		default:
			return nil
		}
		sent = append(sent, byte(r))
	}
	return new(hiveserver2.TPatternOrIdentifier(sent))
}

func (c *thriftClient) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	type getObjectsResult struct {
		tableResp *hiveserver2.TFetchResultsResp
		colResp   *hiveserver2.TFetchResultsResp
	}

	tableMatch, err := sparkbase.NamePattern(tableFilter)
	if err != nil {
		return nil, err
	}
	// Spark's Thrift server matches column names as a case-sensitive regex,
	// so they're matched only on the client.
	columnMatch, err := driverbase.PatternToRegexp(columnFilter)
	if err != nil {
		return nil, err
	}

	// HiveServer2 lacks support for parameters so we have to build the
	// query.  N.B. Databricks extends the protocol with parameters so we
	// should use that when available.
	tablesReq := &hiveserver2.TGetTablesReq{
		SessionHandle: c.session,
		CatalogName:   new(hiveserver2.TPatternOrIdentifier(catalog)),
		SchemaName:    new(hiveserver2.TPatternOrIdentifier(schema)),
		TableName:     tablePattern(tableFilter),
	}

	var colReq *hiveserver2.TGetColumnsReq
	if includeColumns {
		colReq = &hiveserver2.TGetColumnsReq{
			SessionHandle: c.session,
			CatalogName:   new(hiveserver2.TIdentifier(catalog)),
			SchemaName:    new(hiveserver2.TPatternOrIdentifier(schema)),
			TableName:     tablesReq.TableName,
		}
	}

	result, err := driverbase.WithShared(c.client, func(client *hiveserver2.TCLIServiceClient) (*getObjectsResult, error) {
		resp, err := client.GetTables(ctx, tablesReq)
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
		tableRs, err := client.FetchResults(ctx, &hiveserver2.TFetchResultsReq{
			OperationHandle: resp.OperationHandle,
			Orientation:     hiveserver2.TFetchOrientation_FETCH_FIRST,
			MaxRows:         65536,
		})
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, tableRs, "get tables for %s.%s", catalog, schema); err != nil {
			return nil, err
		}

		if colReq == nil {
			return &getObjectsResult{tableResp: tableRs}, nil
		}

		colResp, err := client.GetColumns(ctx, colReq)
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, colResp, "get columns for %s.%s", catalog, schema); err != nil {
			return nil, err
		}

		if !colResp.OperationHandle.HasResultSet {
			return &getObjectsResult{tableResp: tableRs}, nil
		}

		// TODO: may need to fetch results multiple times
		colRs, err := client.FetchResults(ctx, &hiveserver2.TFetchResultsReq{
			OperationHandle: colResp.OperationHandle,
			Orientation:     hiveserver2.TFetchOrientation_FETCH_FIRST,
			MaxRows:         65536,
		})
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, colRs, "get columns for %s.%s", catalog, schema); err != nil {
			return nil, err
		}

		return &getObjectsResult{tableResp: tableRs, colResp: colRs}, nil
	})
	if err != nil {
		return nil, err
	}

	tables := make([]driverbase.TableInfo, 0)
	tableIndex := make(map[string]int)
	for _, row := range result.tableResp.Results.Rows {
		tableName := *row.ColVals[2].GetStringVal().Value
		if !sparkbase.MatchesName(tableMatch, tableName) {
			continue
		}
		tableType := *row.ColVals[3].GetStringVal().Value
		tableIndex[tableName] = len(tables)
		tables = append(tables, driverbase.TableInfo{
			TableName:    tableName,
			TableType:    tableType,
			TableColumns: []driverbase.ColumnInfo{},
		})
	}

	if result.colResp != nil {
		for _, row := range result.colResp.Results.Rows {
			tableName := *row.ColVals[2].GetStringVal().Value
			idx, ok := tableIndex[tableName]
			if !ok {
				continue
			}
			columnName := *row.ColVals[3].GetStringVal().Value
			if !sparkbase.MatchesName(columnMatch, columnName) {
				continue
			}

			col := driverbase.ColumnInfo{
				ColumnName: columnName,
			}

			if v := row.ColVals[16].GetI32Val(); v != nil && v.Value != nil {
				col.OrdinalPosition = new(*v.Value + 1)
			}
			if v := row.ColVals[4].GetI32Val(); v != nil && v.Value != nil {
				col.XdbcDataType = new(int16(*v.Value))
			}
			if v := row.ColVals[5].GetStringVal(); v != nil && v.Value != nil {
				col.XdbcTypeName = v.Value
			}
			if v := row.ColVals[6].GetI32Val(); v != nil && v.Value != nil {
				col.XdbcColumnSize = v.Value
			}
			if v := row.ColVals[10].GetI32Val(); v != nil && v.Value != nil {
				col.XdbcNullable = new(int16(*v.Value))
			}
			if v := row.ColVals[11].GetStringVal(); v != nil && v.Value != nil {
				col.Remarks = v.Value
			}
			if v := row.ColVals[17].GetStringVal(); v != nil && v.Value != nil {
				col.XdbcIsNullable = v.Value
			}

			tables[idx].TableColumns = append(tables[idx].TableColumns, col)
		}
	}

	return tables, nil
}

func (c *thriftClient) GetOption(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}

func (c *thriftClient) GetOptionInt(_ context.Context, _ string) (int64, bool, error) {
	return 0, false, nil
}

var _ sparkbase.SparkClient = (*thriftClient)(nil)
