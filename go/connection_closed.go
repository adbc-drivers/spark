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

package spark

import (
	"context"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
)

// errConnectionClosed reports use of a connection after Close.
var errConnectionClosed = adbc.Error{
	Code: adbc.StatusInvalidState,
	Msg:  "[spark] connection is closed",
}

// closedGuard rejects metadata calls after the connection is closed. Close
// releases the Spark client, and the driverbase helpers keep a reference to
// it, so a call would otherwise fail with a gRPC error or a nil dereference.
type closedGuard struct {
	conn    *connectionImpl
	objects driverbase.DbObjectsEnumerator
}

func (g *closedGuard) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	if g.conn.client == nil {
		return nil, errConnectionClosed
	}
	return g.objects.GetCatalogs(ctx, catalogFilter)
}

func (g *closedGuard) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	if g.conn.client == nil {
		return nil, errConnectionClosed
	}
	return g.objects.GetDBSchemasForCatalog(ctx, catalog, schemaFilter)
}

func (g *closedGuard) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	if g.conn.client == nil {
		return nil, errConnectionClosed
	}
	return g.objects.GetTablesForDBSchema(ctx, catalog, schema, tableFilter, columnFilter, includeColumns)
}
