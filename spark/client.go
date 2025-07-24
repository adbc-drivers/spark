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
	"fmt"
	"io"
	"net/url"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type queryContext struct {
	mem   memory.Allocator
	query string
}

type sparkClient interface {
	io.Closer
	driverbase.DbObjectsEnumerator

	currentCatalog(ctx context.Context) (string, error)
	currentSchema(ctx context.Context) (string, error)
	executeQuery(ctx context.Context, query queryContext) (array.RecordReader, int64, error)
	executeUpdate(ctx context.Context, query queryContext) (int64, error)
	setCurrentCatalog(ctx context.Context, catalog string) error
	setCurrentSchema(ctx context.Context, schema string) error
}

type sparkClientFactory func(context.Context) (sparkClient, error)

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
