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
	"io"
	"net/url"

	"github.com/adbc-drivers/apache/spark/internal/hiveserver2"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/columnar-tech/sasl-go"
)

type queryContext struct {
	mem   memory.Allocator
	query string
}

type sparkClient interface {
	io.Closer

	currentCatalog(ctx context.Context) (string, error)
	currentSchema(ctx context.Context) (string, error)
	executeQuery(ctx context.Context, query queryContext) (array.RecordReader, int64, error)
	executeUpdate(ctx context.Context, query queryContext) (int64, error)
	setCurrentCatalog(ctx context.Context, catalog string) error
	setCurrentSchema(ctx context.Context, schema string) error
}

type sparkClientFactory func(context.Context) (sparkClient, error)

type thriftClient struct {
	transport thrift.TTransport
	// TODO(lidavidm): do we need the lock if we're using the HTTP client?
	client  *driverbase.Shared[hiveserver2.TCLIServiceClient]
	session *hiveserver2.TSessionHandle
}

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

func newThriftHttpClient(ctx context.Context, uri string) (sparkClient, error) {
	cfg := &thrift.TConfiguration{}

	transport, err := thrift.NewTHttpClient(uri)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "open Thrift client")
	}
	// Without further configuration, it seems Spark expects the header
	// but does not do anything with it
	transport.(*thrift.THttpClient).SetHeader("authorization", "Basic dXNlcjo=")

	return wrapThriftTransport(ctx, cfg, transport)
}

func newThriftTcpClient(ctx context.Context, hostPort string) (sparkClient, error) {
	cfg := &thrift.TConfiguration{}

	var transport thrift.TTransport

	transport = thrift.NewTSocketConf(hostPort, cfg)
	// Without further configuration, it seems Spark expects a user/pass
	// but does not do anything with it
	transport = sasl.WrapTransport(transport, "localhost", &sasl.PlainMechanism{
		Username: "u",
		Password: "p",
	})
	if err := transport.Open(); err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err, "open Thrift client")
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

func newSparkClientFactory(options map[string]string) (func(context.Context) (sparkClient, error), error) {
	uri, ok := options[adbc.OptionKeyURI]
	if !ok {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[spark] missing required option: " + adbc.OptionKeyURI,
		}
	}
	delete(options, adbc.OptionKeyURI)

	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
	}

	switch parsed.Scheme {
	case "grpc":
		return nil, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("[spark] Spark Connect not yet supported: %s", uri),
		}

	case "http":
		baseURI := fmt.Sprintf("http://%s/cliservice", parsed.Host)
		return func(ctx context.Context) (sparkClient, error) {
			return newThriftHttpClient(ctx, baseURI)
		}, nil

	case "thrift":
		return func(ctx context.Context) (sparkClient, error) {
			return newThriftTcpClient(ctx, parsed.Host)
		}, nil

	}
	return nil, adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("[spark] unknown connection type: %s", uri),
	}
}

var _ sparkClient = (*thriftClient)(nil)
