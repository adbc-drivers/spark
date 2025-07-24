// Copyright (c) 2025 Columnar Technologies, Inc.
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
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/apache/spark/internal/hiveserver2"
	"github.com/apache/arrow-adbc/go/adbc"
)

var errTBD = adbc.Error{
	Code: adbc.StatusNotImplemented,
	Msg:  "[spark] TBD",
}

// errToAdbcErr converts an error to an ADBC error.
func errToAdbcErr(defaultStatus adbc.Status, err error, context string, contextArgs ...any) error {
	var adbcError adbc.Error
	if errors.As(err, &adbcError) {
		return err
	}

	status := defaultStatus
	var details []adbc.ErrorDetail
	var sqlState [5]byte

	var builder strings.Builder
	var trailers []string

	if builder.Len() == 0 {
		builder.WriteString(err.Error())
	}

	if len(trailers) > 0 {
		builder.WriteString(" (")
		for i, trailer := range trailers {
			if i > 0 {
				builder.WriteString("; ")
			}
			builder.WriteString(trailer)
		}
		builder.WriteString(")")
	}

	return adbc.Error{
		Code:     status,
		Msg:      fmt.Sprintf("[spark] Could not %s: %s", fmt.Sprintf(context, contextArgs...), builder.String()),
		SqlState: sqlState,
		Details:  details,
	}
}

func statusToAdbcErr(status *hiveserver2.TStatus, context string, contextArgs ...any) error {
	switch status.StatusCode {
	case hiveserver2.TStatusCode_SUCCESS_STATUS:
		return nil
	case hiveserver2.TStatusCode_SUCCESS_WITH_INFO_STATUS:
		// TODO: issue a warning?
		return nil
	}

	return adbc.Error{
		Code: adbc.StatusIO,
		Msg:  fmt.Sprintf("[spark] Could not %s: %s", fmt.Sprintf(context, contextArgs...), status),
	}
}

type getStatus interface {
	GetStatus() *hiveserver2.TStatus
}

func toAdbcErr(defaultStatus adbc.Status, err error, status getStatus, context string, contextArgs ...any) error {
	if err != nil {
		return errToAdbcErr(defaultStatus, err, context, contextArgs...)
	} else if status != nil {
		return statusToAdbcErr(status.GetStatus(), context, contextArgs...)
	}
	return nil
}

// func quoteIdentifier(ident string) string {
// 	// TODO:
// 	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(ident, `"`, `""`))
// }

// func quoteString(value string) string {
// 	// TODO:
// 	return fmt.Sprintf(`'%s'`, strings.ReplaceAll(value, `'`, `''`))
// }
