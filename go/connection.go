// Copyright (c) 2025-2026 ADBC Drivers Contributors
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
	"fmt"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	client         sparkbase.SparkClient
	s3BaseEndpoint string
	s3UsePathStyle bool
}

func (c *connectionImpl) Init(client sparkbase.SparkClient) error {
	c.client = client
	return nil
}

func (c *connectionImpl) Close(ctx context.Context) error {
	if c.client == nil {
		return c.ErrorHelper.Errorf(adbc.StatusInvalidState, "connection not initialized or already closed")
	}

	if err := c.client.Close(ctx); err != nil {
		// Prefer to log/warn so that connection object can be closed
		c.Logger.WarnContext(ctx, "error closing spark client", "err", err)
		return nil
	}
	c.client = nil
	return nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	backendName := c.client.BackendName()
	if version, err := c.client.VendorVersion(ctx, c.Alloc); err == nil && version != "" {
		fullVersion := fmt.Sprintf("%s (%s)", version, backendName)
		if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, fullVersion); err != nil {
			return err
		}
	}
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

func (c *connectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	return c.client.CurrentCatalog(ctx, c.Alloc)
}

func (c *connectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	return c.client.CurrentSchema(ctx, c.Alloc)
}

func (c *connectionImpl) SetCurrentCatalog(ctx context.Context, value string) error {
	return c.client.SetCurrentCatalog(ctx, c.Alloc, value)
}

func (c *connectionImpl) SetCurrentDbSchema(ctx context.Context, value string) error {
	return c.client.SetCurrentSchema(ctx, c.Alloc, value)
}

func (c *connectionImpl) SetAutocommit(ctx context.Context, enabled bool) error {
	if enabled {
		return nil
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[spark] Cannot disable autocommit",
	}
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	parts := make([]string, 0, 3)
	if catalog != nil && *catalog != "" {
		parts = append(parts, sparkbase.QuoteIdentifier(*catalog))
	}
	if dbSchema != nil && *dbSchema != "" {
		parts = append(parts, sparkbase.QuoteIdentifier(*dbSchema))
	}
	parts = append(parts, sparkbase.QuoteIdentifier(tableName))

	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", strings.Join(parts, "."))
	rdr, _, err := c.client.ExecuteQuery(ctx, sparkbase.QueryContext{
		Mem:   c.Alloc,
		Log:   c.Logger,
		Query: query,
	})
	if err != nil {
		return nil, err
	}
	defer rdr.Release()

	return rdr.Schema(), nil
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

func (c *connectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	return driverbase.NewStatement(&statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase, c.ErrorHelper),
		cnxn:              c,
		ingest: bulkIngestOptions{
			BulkIngestOptions: driverbase.NewBulkIngestOptions(),
			s3BaseEndpoint:    c.s3BaseEndpoint,
			s3UsePathStyle:    c.s3UsePathStyle,
		},
	}), nil
}

func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "[spark] ReadPartition not supported",
	}
}

func (c *connectionImpl) GetOption(ctx context.Context, key string) (string, error) {
	if val, ok, err := c.client.GetOption(ctx, key); err != nil {
		return "", err
	} else if ok {
		return val, nil
	}
	return c.ConnectionImplBase.GetOption(ctx, key)
}

func (c *connectionImpl) GetOptionInt(ctx context.Context, key string) (int64, error) {
	if val, ok, err := c.client.GetOptionInt(ctx, key); err != nil {
		return 0, err
	} else if ok {
		return val, nil
	}
	return c.ConnectionImplBase.GetOptionInt(ctx, key)
}
