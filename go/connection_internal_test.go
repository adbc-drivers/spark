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

	"github.com/stretchr/testify/require"
)

func TestListTableTypes(t *testing.T) {
	// TABLE and TEMPORARY, which GetObjects reports, come first; the types
	// listed before stay listed, in the same order.
	types, err := (&connectionImpl{}).ListTableTypes(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{
		"TABLE",
		"TEMPORARY",
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
