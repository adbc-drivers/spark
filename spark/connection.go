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

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	client sparkClient
}

func (c *connectionImpl) Init(client sparkClient) error {
	c.client = client
	return nil
}

func (c *connectionImpl) Close() error {
	if c.client == nil {
		return c.ErrorHelper.Errorf(adbc.StatusInvalidState, "connection not initialized or already closed")
	}

	if err := c.client.Close(); err != nil {
		return err
	}
	c.client = nil
	return nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false)
}

func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{
		"VIEW",
		"FOREIGN",
		"MANAGED",
		"STREAMING_TABLE",
		"MATERIALIZED_VIEW",
		"EXTERNAL",
		"MANAGED_SHALLOW_CLONE",
		"EXTERNAL_SHALLOW_CLONE",
	}, nil
}

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.client.currentCatalog(context.Background())
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.client.currentSchema(context.Background())
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
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[spark] Transactions not supported",
	}
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[spark] Transactions not supported",
	}
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
