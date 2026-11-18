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
	"testing"

	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// The connection is opened lazily, so none of these calls reach a server. The
// address is closed on purpose: a call that does reach it would fail with a
// gRPC error, not InvalidState.
func TestConnectionCallsAfterCloseReturnInvalidState(t *testing.T) {
	ctx := context.Background()
	db, err := NewDriver(memory.NewGoAllocator()).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI: "sc://localhost:1?api=connect&auth_type=none&tls=false",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(ctx) })

	conn, err := db.Open(ctx)
	require.NoError(t, err)

	require.NoError(t, conn.Close(ctx))

	// A statement keeps its connection, so one made before the connection's
	// Close must not reach the released client either.
	statementOfClosedConnection := func() adbc.StatementWithContext {
		c, err := db.Open(ctx)
		require.NoError(t, err)
		stmt, err := c.NewStatement(ctx)
		require.NoError(t, err)
		require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 1"))
		require.NoError(t, c.Close(ctx))
		return stmt
	}

	calls := map[string]func() error{
		"ExecuteQuery": func() error {
			stmt := statementOfClosedConnection()
			defer testutil.CheckedCloseWithContext(t, stmt, ctx)
			_, _, err := stmt.ExecuteQuery(ctx)
			return err
		},
		"ExecuteUpdate": func() error {
			stmt := statementOfClosedConnection()
			defer testutil.CheckedCloseWithContext(t, stmt, ctx)
			_, err := stmt.ExecuteUpdate(ctx)
			return err
		},
		"GetObjects": func() error {
			_, err := conn.GetObjects(ctx, adbc.ObjectDepthTables, nil, nil, nil, nil, nil)
			return err
		},
		"GetTableTypes": func() error {
			_, err := conn.GetTableTypes(ctx)
			return err
		},
		"GetTableSchema": func() error {
			_, err := conn.GetTableSchema(ctx, nil, nil, "t")
			return err
		},
		"GetInfo": func() error {
			_, err := conn.GetInfo(ctx, nil)
			return err
		},
		"NewStatement": func() error {
			_, err := conn.NewStatement(ctx)
			return err
		},
		// The current catalog and schema are reached through their options.
		"GetCurrentCatalog": func() error {
			_, err := conn.(adbc.GetSetOptionsWithContext).GetOption(ctx, adbc.OptionKeyCurrentCatalog)
			return err
		},
		"GetCurrentDbSchema": func() error {
			_, err := conn.(adbc.GetSetOptionsWithContext).GetOption(ctx, adbc.OptionKeyCurrentDbSchema)
			return err
		},
		"SetCurrentCatalog": func() error {
			return conn.(adbc.GetSetOptionsWithContext).SetOption(ctx, adbc.OptionKeyCurrentCatalog, "c")
		},
		"GetOption": func() error {
			_, err := conn.(adbc.GetSetOptionsWithContext).GetOption(ctx, "spark.connect.session_id")
			return err
		},
		"GetOptionInt": func() error {
			_, err := conn.(adbc.GetSetOptionsWithContext).GetOptionInt(ctx, "spark.connect.session_id")
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			var err error
			require.NotPanics(t, func() { err = call() })
			require.ErrorContains(t, err, "connection is closed")
			// Driverbase wraps errors from the current catalog and schema
			// options as NotFound, so only the message is checked for those.
			if name == "GetCurrentCatalog" || name == "GetCurrentDbSchema" {
				return
			}
			var adbcErr adbc.Error
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusInvalidState, adbcErr.Code, "unexpected error: %v", err)
		})
	}

	// Closing twice still reports an error.
	require.Error(t, conn.Close(ctx))
}
