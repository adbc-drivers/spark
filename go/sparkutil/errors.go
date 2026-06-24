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

package sparkutil

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

const errPrefix = "[spark]"

func ToAdbcErr(defaultStatus adbc.Status, err error, context string, contextArgs ...any) error {
	if adbcErr, ok := errors.AsType[adbc.Error](err); ok {
		msg, _ := strings.CutPrefix(adbcErr.Msg, errPrefix)
		msg = fmt.Sprintf("%s %s: %s", errPrefix, fmt.Sprintf(context, contextArgs...), msg)
		adbcErr.Msg = msg
	}
	return err
}

func HttpStatusToCode(statusCode int) adbc.Status {
	switch {
	case statusCode >= 400 && statusCode < 500:
		return adbc.StatusInvalidArgument
	case statusCode >= 500 && statusCode < 600:
		return adbc.StatusInternal
	default:
		return adbc.StatusUnknown
	}
}
