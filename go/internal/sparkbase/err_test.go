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

package sparkbase_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/adbc-drivers/spark/go/internal/connectimpl"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sparkError returns the Spark Connect client's error for a failed RPC, with
// the SQLSTATE in an ErrorInfo as Spark sends it.
func sparkError(t *testing.T, code codes.Code, sqlState string) error {
	t.Helper()
	st := status.New(code, "boom")
	if sqlState != "" {
		var err error
		st, err = st.WithDetails(&errdetails.ErrorInfo{Metadata: map[string]string{"sqlState": sqlState}})
		require.NoError(t, err)
	}
	return connectimpl.NewSparkError(st.Err())
}

func TestErrToAdbcErrUsesSparkSQLState(t *testing.T) {
	err := sparkbase.ErrToAdbcErr(adbc.StatusIO, fmt.Errorf("execute: %w", sparkError(t, codes.Internal, "42P01")), "execute query")

	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
	require.Equal(t, [5]byte{'4', '2', 'P', '0', '1'}, adbcErr.SqlState)
}

func TestErrToAdbcErrMapsGRPCStatus(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want adbc.Status
	}{
		{codes.Canceled, adbc.StatusCancelled},
		{codes.DeadlineExceeded, adbc.StatusTimeout},
		{codes.InvalidArgument, adbc.StatusInvalidArgument},
		{codes.NotFound, adbc.StatusNotFound},
		{codes.AlreadyExists, adbc.StatusAlreadyExists},
		{codes.Unauthenticated, adbc.StatusUnauthenticated},
		{codes.PermissionDenied, adbc.StatusUnauthorized},
		{codes.Unimplemented, adbc.StatusNotImplemented},
		{codes.Unavailable, adbc.StatusIO},
		// Codes without a more specific status keep the caller's default.
		{codes.Internal, adbc.StatusInvalidState},
		{codes.ResourceExhausted, adbc.StatusInvalidState},
		{codes.Unknown, adbc.StatusInvalidState},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			for _, err := range []error{
				fmt.Errorf("execute: %w", status.Error(tc.code, "boom")),
				sparkError(t, tc.code, ""),
			} {
				var adbcErr adbc.Error
				require.ErrorAs(t, sparkbase.ErrToAdbcErr(adbc.StatusInvalidState, err, "execute query"), &adbcErr)
				require.Equal(t, tc.want, adbcErr.Code)
			}
		})
	}
}

func TestErrToAdbcErrPrefersSQLStateOverGRPCStatus(t *testing.T) {
	err := sparkError(t, codes.InvalidArgument, "42P01")

	var adbcErr adbc.Error
	require.ErrorAs(t, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute query"), &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
}

func TestErrToAdbcErrMapsContextErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want adbc.Status
	}{
		{fmt.Errorf("execute: %w", context.Canceled), adbc.StatusCancelled},
		{context.DeadlineExceeded, adbc.StatusTimeout},
		{errors.New("boom"), adbc.StatusIO},
	} {
		var adbcErr adbc.Error
		require.ErrorAs(t, sparkbase.ErrToAdbcErr(adbc.StatusIO, tc.err, "execute query"), &adbcErr)
		require.Equal(t, tc.want, adbcErr.Code, tc.err.Error())
	}
}
