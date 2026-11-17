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

package thriftimpl

import (
	"context"
	"testing"

	connectpb "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// Bound parameters are rejected before any request is sent, so a client with
// no session is enough to test it.
func TestExecuteRejectsPosArgs(t *testing.T) {
	c := &thriftClient{}
	q := sparkbase.QueryContext{
		Mem:     memory.DefaultAllocator,
		Query:   "SELECT ?",
		PosArgs: []*connectpb.Expression_Literal{{}},
	}
	_, _, err := c.ExecuteQuery(context.Background(), q)
	require.ErrorAs(t, err, new(adbc.Error))
	_, err = c.ExecuteUpdate(context.Background(), q)
	require.ErrorAs(t, err, new(adbc.Error))
}
