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
	"net/url"

	"github.com/adbc-drivers/apache/spark/internal/hiveserver2"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/thrift/lib/go/thrift"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	transport thrift.TTransport
	// TODO: this will need more abstraction so that we can also support
	// Spark Connect and Livy.  Also in theory I think the lock is
	// unnecessary unless it is Thrift-over-TCP.
	client  *driverbase.Shared[hiveserver2.TCLIServiceClient]
	session *hiveserver2.TSessionHandle
}

type nilCloser struct{}

func (nilCloser) Close() error { return nil }

func (c *connectionImpl) Init(ctx context.Context, uri *url.URL) error {
	cfg := &thrift.TConfiguration{}
	// c.transport = thrift.NewTSocketConf(uri.Host, cfg)

	// if err := c.transport.Open(); err != nil {
	// 	return errToAdbcErr(adbc.StatusIO, err, "open thrift transport")
	// }

	var err error
	c.transport, err = thrift.NewTHttpClient("http://localhost:10001/cliservice")
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "open Thrift client")
	}
	c.transport.(*thrift.THttpClient).SetHeader("authorization", "Basic dXNlcjo=")

	factory := thrift.NewTBinaryProtocolFactoryConf(cfg)
	iprot := factory.GetProtocol(c.transport)
	oprot := factory.GetProtocol(c.transport)

	client := hiveserver2.NewTCLIServiceClient(thrift.NewTStandardClient(iprot, oprot))

	req := &hiveserver2.TOpenSessionReq{}
	resp, err := client.OpenSession(ctx, req)
	if err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "open HiveServer2 session")
	} else if err = statusToAdbcErr(resp.Status, "open HiveServer2 session"); err != nil {
		return err
	}
	c.client = driverbase.NewShared(client, nilCloser{})
	c.session = resp.SessionHandle
	return nil
}

func (c *connectionImpl) Close() error {
	if c.transport == nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[spark] connection not initialized or already closed",
		}
	}

	// TODO: close session

	if err := c.transport.Close(); err != nil {
		return errToAdbcErr(adbc.StatusIO, err, "close thrift transport")
	}
	c.transport = nil
	return nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false)
}

func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// TODO:
	return []string{"TABLE", "VIEW"}, nil
}

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return "", errTBD
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return "", errTBD
}

func (c *connectionImpl) SetCurrentCatalog(value string) error {
	return errTBD
}

func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	return errTBD
}

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled {
		return nil
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[spark] Cannot disable autocommit",
	}
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, errTBD
}

func (c *connectionImpl) Commit(ctx context.Context) error {
	return errTBD
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	return errTBD
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		cnxn:              c,
	}, nil
}

func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[spark] ReadPartition not supported",
	}
}

func (c *connectionImpl) SetOption(key, value string) error {
	switch key {
	default:
		return adbc.Error{
			Msg:  "[spark] unknown connection option " + key + ": " + value,
			Code: adbc.StatusInvalidArgument,
		}
	}
}
