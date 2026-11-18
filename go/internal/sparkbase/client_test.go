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

package sparkbase

import (
	"testing"

	connectpb "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestRejectPosArgs(t *testing.T) {
	require.NoError(t, RejectPosArgs("HiveServer2+binary", QueryContext{Query: "SELECT 1"}))

	err := RejectPosArgs("HiveServer2+binary", QueryContext{
		Query:   "SELECT ?",
		PosArgs: []*connectpb.Expression_Literal{{}},
	})
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
}
