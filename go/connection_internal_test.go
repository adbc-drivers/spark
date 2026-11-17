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

	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/stretchr/testify/require"
)

// openClient stands in for the Spark client of an open connection.
// ListTableTypes doesn't call it.
type openClient struct{ sparkbase.SparkClient }

func TestListTableTypes(t *testing.T) {
	// TABLE is the fallback type of a table with no Type: line, so GetTableTypes
	// lists it. The types listed before stay listed, in the same order.
	conn := &connectionImpl{client: openClient{}}
	types, err := conn.ListTableTypes(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{
		"TABLE",
		"VIEW",
		"FOREIGN",
		"MANAGED",
		"STREAMING_TABLE",
		"MATERIALIZED_VIEW",
		"EXTERNAL",
		"MANAGED_SHALLOW_CLONE",
		"EXTERNAL_SHALLOW_CLONE",
	}, types)
}
