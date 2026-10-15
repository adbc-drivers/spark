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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sparkErr is shaped like a Spark Connect error: a gRPC status plus SQLSTATE.
type sparkErr struct {
	code     codes.Code
	sqlState string
}

func (e sparkErr) Error() string { return "boom" }

func (e sparkErr) GRPCStatus() *status.Status { return status.New(e.code, "boom") }

func (e sparkErr) SQLState() string { return e.sqlState }

func TestErrToAdbcErrUsesSparkSQLState(t *testing.T) {
	err := ErrToAdbcErr(adbc.StatusIO, fmt.Errorf("execute: %w", sparkErr{
		code:     codes.Internal,
		sqlState: "42P01",
	}), "execute query")

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
				sparkErr{code: tc.code},
			} {
				var adbcErr adbc.Error
				require.ErrorAs(t, ErrToAdbcErr(adbc.StatusInvalidState, err, "execute query"), &adbcErr)
				require.Equal(t, tc.want, adbcErr.Code)
			}
		})
	}
}

func TestErrToAdbcErrPrefersSQLStateOverGRPCStatus(t *testing.T) {
	err := sparkErr{code: codes.InvalidArgument, sqlState: "42P01"}

	var adbcErr adbc.Error
	require.ErrorAs(t, ErrToAdbcErr(adbc.StatusIO, err, "execute query"), &adbcErr)
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
		require.ErrorAs(t, ErrToAdbcErr(adbc.StatusIO, tc.err, "execute query"), &adbcErr)
		require.Equal(t, tc.want, adbcErr.Code, tc.err.Error())
	}
}
