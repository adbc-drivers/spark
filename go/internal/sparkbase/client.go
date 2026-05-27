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
	"fmt"
	"io"
	"log/slog"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type QueryContext struct {
	ReaderOptions driverbase.BaseRecordReaderOptions
	Mem           memory.Allocator
	Log           *slog.Logger
	Query         string
}

type SparkClient interface {
	io.Closer
	driverbase.DbObjectsEnumerator

	ExecuteQuery(ctx context.Context, query QueryContext) (array.RecordReader, int64, error)
	ExecuteUpdate(ctx context.Context, query QueryContext) (int64, error)

	CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error)
	SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error

	CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error)
	SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error

	VendorVersion(ctx context.Context, mem memory.Allocator) (string, error)
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
		return string(stringCol.Value(0)), nil
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
	sql := fmt.Sprintf("USE CATALOG %s", QuoteString(catalog))
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
	sql := fmt.Sprintf("USE SCHEMA %s", QuoteString(schema))
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
