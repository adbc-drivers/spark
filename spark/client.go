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
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type queryContext struct {
	mem   memory.Allocator
	query string
}

type sparkClient interface {
	io.Closer
	driverbase.DbObjectsEnumerator

	currentCatalog(ctx context.Context) (string, error)
	currentSchema(ctx context.Context) (string, error)
	executeQuery(ctx context.Context, query queryContext) (array.RecordReader, int64, error)
	executeUpdate(ctx context.Context, query queryContext) (int64, error)
	setCurrentCatalog(ctx context.Context, catalog string) error
	setCurrentSchema(ctx context.Context, schema string) error
}

type sparkClientFactory func(context.Context) (sparkClient, error)

func invalidOptionErr(option string, value string) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("[spark] invalid option value '%s' for option %s", value, option),
	}
}

func missingRequiredOptionErr(option string) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  "[spark] missing required option: " + option,
	}
}

func parseOptionsFromUri(uri *url.URL, options map[string]string) error {
	split := strings.Split(uri.Host, ":")
	switch len(split) {
	case 1:
		options[OptionHost] = split[0]
	case 2:
		options[OptionHost] = split[0]
		options[OptionPort] = split[1]
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "Invalid URI host:port",
		}
	}

	queryValues, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return errToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI query")
	}

	for key, values := range queryValues {
		fullKey := fmt.Sprintf("spark.%s", key)
		if len(values) != 1 {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("Key '%s' needs to have exactly one value", key),
			}
		}
		options[fullKey] = values[0]
	}

	options[OptionApi] = uri.Scheme

	return nil
}

func thriftOptsFromOptions(options map[string]string) (thriftConnectionOpts, error) {
	thriftOpts := thriftConnectionOpts{}

	authType, ok := options[OptionAuthType]
	if !ok {
		return thriftOpts, missingRequiredOptionErr(OptionAuthType)
	}
	delete(options, OptionAuthType)

	host, ok := options[OptionHost]
	if !ok {
		return thriftOpts, missingRequiredOptionErr(OptionHost)
	}
	delete(options, OptionHost)
	thriftOpts.host = host

	if port, hasPort := options[OptionPort]; hasPort {
		delete(options, OptionPort)
		thriftOpts.host = fmt.Sprintf("%s:%s", host, port)
	} else {
		thriftOpts.host = host
	}

	switch authType {
	case OptionValueAuthTypeNoSasl:
		thriftOpts.auth = noSasl
	case OptionValueAuthTypePlain:
		thriftOpts.auth = plain

		username, _ := options[adbc.OptionKeyUsername]
		thriftOpts.username = username
		delete(options, adbc.OptionKeyUsername)

		password, _ := options[adbc.OptionKeyPassword]
		thriftOpts.password = password
		delete(options, adbc.OptionKeyPassword)

	case OptionValueAuthTypeLdap, OptionValueAuthTypeKerberos:
		return thriftOpts, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[spark] auth type '%s' has not been implemented yet", authType),
		}
	default:
		return thriftOpts, invalidOptionErr(OptionAuthType, authType)
	}

	return thriftOpts, nil
}

func newSparkClientFactory(options map[string]string) (func(context.Context) (sparkClient, error), error) {
	uri, ok := options[adbc.OptionKeyURI]
	if ok {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
		}

		err = parseOptionsFromUri(parsed, options)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
		}

		delete(options, adbc.OptionKeyURI)
	}

	api, ok := options[OptionApi]
	if !ok {
		return nil, missingRequiredOptionErr(OptionApi)
	}
	delete(options, OptionApi)

	switch api {
	case OptionValueApiThriftBinary, OptionValueApiThriftHttp:
		thriftOpts, err := thriftOptsFromOptions(options)
		if err != nil {
			return nil, err
		}

		if api == OptionValueApiThriftBinary {
			thriftOpts.transport = binary
		} else {
			thriftOpts.transport = http
		}

		return func(ctx context.Context) (sparkClient, error) {
			return newThriftClient(ctx, thriftOpts)
		}, nil

	case OptionValueApiLivy:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[spark] Livy is not supported yet",
		}

	default:
		return nil, invalidOptionErr(OptionApi, api)
	}
}
