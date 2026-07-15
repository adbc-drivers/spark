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

	"github.com/adbc-drivers/spark/go/connectclient/sparkerrors"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestErrToAdbcErrUsesSparkSQLState(t *testing.T) {
	sparkErr := &sparkerrors.SparkError{
		SqlState: "42P01",
		Message:  "table not found",
	}
	err := ErrToAdbcErr(adbc.StatusIO, sparkerrors.WithType(sparkErr, sparkerrors.ExecutionError), "execute query")

	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
	require.Equal(t, [5]byte{'4', '2', 'P', '0', '1'}, adbcErr.SqlState)
}
