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
	"github.com/adbc-drivers/apache/spark/internal/sparkbase"
	"github.com/adbc-drivers/apache/spark/internal/thriftimpl"
	"github.com/apache/arrow-adbc/go/adbc"
	"net/url"
	"strings"
)

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
		return sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI query")
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

func thriftOptsFromOptions(options map[string]string) (thriftimpl.ConnectionOpts, error) {
	thriftOpts := thriftimpl.ConnectionOpts{}

	authType, ok := options[OptionAuthType]
	if !ok {
		return thriftOpts, sparkbase.MissingRequiredOptionErr(OptionAuthType)
	}
	delete(options, OptionAuthType)

	host, ok := options[OptionHost]
	if !ok {
		return thriftOpts, sparkbase.MissingRequiredOptionErr(OptionHost)
	}
	delete(options, OptionHost)
	thriftOpts.Host = host

	if port, hasPort := options[OptionPort]; hasPort {
		delete(options, OptionPort)
		thriftOpts.Host = fmt.Sprintf("%s:%s", host, port)
	} else {
		thriftOpts.Host = host
	}

	switch authType {
	case OptionValueAuthTypeNoSasl:
		thriftOpts.Auth = thriftimpl.NoSasl
	case OptionValueAuthTypePlain:
		thriftOpts.Auth = thriftimpl.Plain

		username, _ := options[adbc.OptionKeyUsername]
		thriftOpts.Username = username
		delete(options, adbc.OptionKeyUsername)

		password, _ := options[adbc.OptionKeyPassword]
		thriftOpts.Password = password
		delete(options, adbc.OptionKeyPassword)

	case OptionValueAuthTypeLdap, OptionValueAuthTypeKerberos:
		return thriftOpts, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[spark] auth type '%s' has not been implemented yet", authType),
		}
	default:
		return thriftOpts, sparkbase.InvalidOptionErr(OptionAuthType, authType)
	}

	return thriftOpts, nil
}

func newSparkClientFactory(options map[string]string) (func(context.Context) (sparkbase.SparkClient, error), error) {
	uri, ok := options[adbc.OptionKeyURI]
	if ok {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
		}

		err = parseOptionsFromUri(parsed, options)
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse URI")
		}

		delete(options, adbc.OptionKeyURI)
	}

	api, ok := options[OptionApi]
	if !ok {
		return nil, sparkbase.MissingRequiredOptionErr(OptionApi)
	}
	delete(options, OptionApi)

	switch api {
	case OptionValueApiThriftBinary, OptionValueApiThriftHttp:
		thriftOpts, err := thriftOptsFromOptions(options)
		if err != nil {
			return nil, err
		}

		if api == OptionValueApiThriftBinary {
			thriftOpts.Transport = thriftimpl.Binary
		} else {
			thriftOpts.Transport = thriftimpl.Http
		}

		return func(ctx context.Context) (sparkbase.SparkClient, error) {
			return thriftimpl.NewClient(ctx, thriftOpts)
		}, nil

	case OptionValueApiLivy:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "[spark] Livy is not supported yet",
		}

	default:
		return nil, sparkbase.InvalidOptionErr(OptionApi, api)
	}
}
