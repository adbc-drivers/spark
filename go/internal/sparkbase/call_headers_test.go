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
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestCallHeadersValidation(t *testing.T) {
	var headers CallHeaders
	for _, name := range []string{"", "bad name", ":authority", "grpc-timeout", "data-bin", "a/b", "é", "K", "GRPC-timeout", "X-BIN"} {
		err := headers.SetCallHeader(name, "secret-sentinel")
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
		require.NotContains(t, err.Error(), "secret-sentinel")
		_, err = headers.GetCallHeader(name)
		require.Error(t, err)
	}
	for _, value := range []string{"secret\n", "secret\r", "secret\t", "secret\x00", "secret\x7f", "secreté"} {
		err := headers.SetCallHeader("x-valid", value)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
	require.NoError(t, headers.SetCallHeader("X-A_1.test", " ~"))
	snapshot := headers.Snapshot()
	require.NoError(t, headers.SetCallHeader("x-a_1.test", ""))
	require.Equal(t, []string{" ~"}, snapshot.Get("x-a_1.test"))
	value, err := headers.GetCallHeader("X-A_1.test")
	require.NoError(t, err)
	require.Empty(t, value)
	snapshot.Set("x-a_1.test", "changed")
	value, err = headers.GetCallHeader("x-a_1.test")
	require.NoError(t, err)
	require.Empty(t, value)
}

func TestCallHeadersConcurrentSnapshots(t *testing.T) {
	var headers CallHeaders
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				require.NoError(t, headers.SetCallHeader("x-route", "value"))
				_, err := headers.GetCallHeader("x-route")
				require.NoError(t, err)
				headers.Snapshot().Set("x-route", "snapshot")
			}
		})
	}
	wg.Wait()
}
