// Copyright (c) 2025 ADBC Drivers Contributors
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
	"errors"
	"fmt"
	"strings"

	"github.com/adbc-drivers/spark/go/internal/hiveserver2"
	"github.com/apache/arrow-adbc/go/adbc"
)

var ErrTBD = adbc.Error{
	Code: adbc.StatusNotImplemented,
	Msg:  "[spark] TBD",
}

// errToAdbcErr converts an error to an ADBC error.
func ErrToAdbcErr(defaultStatus adbc.Status, err error, context string, contextArgs ...any) error {
	if _, ok := errors.AsType[adbc.Error](err); ok {
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

func StatusToAdbcErr(defaultStatus adbc.Status, status *hiveserver2.TStatus, context string, contextArgs ...any) error {
	switch status.StatusCode {
	case hiveserver2.TStatusCode_SUCCESS_STATUS:
		return nil
	case hiveserver2.TStatusCode_SUCCESS_WITH_INFO_STATUS:
		// TODO: issue a warning?
		return nil
	}

	messages := strings.Join(status.InfoMessages, "\n")
	err := adbc.Error{
		Code: defaultStatus,
		Msg:  fmt.Sprintf("[spark] could not %s: %s", fmt.Sprintf(context, contextArgs...), messages),
	}

	if strings.Contains(messages, "Failed to get table info from metastore") {
		// Combined Hive-Iceberg metastore doesn't set SQLSTATE for this
		err.Code = adbc.StatusNotFound
		err.SqlState = [5]byte{'4', '2', 'P', '0', '1'}
	}

	if status.SqlState != nil {
		copy(err.SqlState[:], []byte(*status.SqlState))

		// https://spark.apache.org/docs/3.5.8/sql-error-conditions.html
		switch *status.SqlState {
		case "22003":
			err.Code = adbc.StatusInvalidData
		case "22546", "42000", "42601", "42702", "42704", "42710", "42846":
			err.Code = adbc.StatusInvalidArgument
		case "42K03", "42P01":
			err.Code = adbc.StatusNotFound
		case "42P07":
			err.Code = adbc.StatusAlreadyExists
		}
	}
	if status.ErrorCode != nil {
		err.VendorCode = *status.ErrorCode
	}

	return err
}

type GetStatus interface {
	GetStatus() *hiveserver2.TStatus
}

func QuoteIdentifier(ident string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(ident, "`", "``"))
}

func QuoteString(value string) string {
	return fmt.Sprintf(`'%s'`, strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`))
}

func InvalidOptionErr(option string, value string) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("[spark] invalid option value '%s' for option %s", value, option),
	}
}

func MissingRequiredOptionErr(option string) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  "[spark] missing required option: " + option,
	}
}

func ToAdbcErr(defaultStatus adbc.Status, err error, status GetStatus, context string, contextArgs ...any) error {
	if err != nil {
		return ErrToAdbcErr(defaultStatus, err, context, contextArgs...)
	} else if status != nil {
		return StatusToAdbcErr(defaultStatus, status.GetStatus(), context, contextArgs...)
	}
	return nil
}
