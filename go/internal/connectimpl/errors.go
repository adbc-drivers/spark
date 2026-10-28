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

package connectimpl

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SparkError is a failed Spark Connect RPC. It keeps the gRPC status so the
// code can be mapped, and the SQLSTATE that Spark attaches in an ErrorInfo.
type SparkError struct {
	status   *status.Status
	sqlState string
}

// NewSparkError wraps a gRPC error; other errors are returned unchanged.
func NewSparkError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	e := &SparkError{status: st}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			e.sqlState = info.GetMetadata()["sqlState"]
		}
	}
	return e
}

func (e *SparkError) Error() string {
	// Spark's own errors already start with their error class.
	if e.status.Code() == codes.Internal {
		return e.status.Message()
	}
	return fmt.Sprintf("[%s] %s", e.status.Code(), e.status.Message())
}

func (e *SparkError) GRPCStatus() *status.Status { return e.status }

func (e *SparkError) SQLState() string { return e.sqlState }
