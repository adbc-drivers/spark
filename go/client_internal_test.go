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

package spark

import (
	"context"
	"net/url"
	"testing"

	"github.com/adbc-drivers/spark/go/internal/connectimpl"
	"github.com/adbc-drivers/spark/go/internal/livyimpl"
	"github.com/adbc-drivers/spark/go/internal/thriftimpl"
	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

func TestParseOptionsFromUri(t *testing.T) {
	type testCase struct {
		uri     string
		options map[string]string
	}

	for _, tc := range []testCase{
		{
			uri: "spark://localhost:10000",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "thrift+binary",
			},
		},
		{
			uri: "spark://localhost:10000?api=livy",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "livy",
			},
		},
		{
			uri: "sc://localhost:10000",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "connect",
			},
		},
		{
			uri: "livy://localhost:10000",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "livy",
			},
		},
		{
			uri: "thrift+binary://localhost:10000",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "thrift+binary",
			},
		},
		{
			uri: "thrift+http://localhost:10000",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "thrift+http",
			},
		},
		{
			uri: "thrift+http://localhost:10000?api=thrift%2Bhttp",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "10000",
				"spark.api":  "thrift+http",
			},
		},
		{
			uri: "livy://localhost",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "livy",
			},
		},
		{
			uri: "livy://localhost?api=livy",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "livy",
			},
		},
		{
			uri: "sc://localhost?api=connect",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "connect",
			},
		},
		{
			uri: "spark://localhost?api=connect&tls=true",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "connect",
				"spark.tls":  "true",
			},
		},
		{
			uri: "spark://localhost?api=connect&tls=true&validateservercertificate=false",
			options: map[string]string{
				"spark.host":                        "localhost",
				"spark.api":                         "connect",
				"spark.tls":                         "true",
				"spark.validate_server_certificate": "false",
			},
		},
		{
			uri: "spark://foobar@localhost?api=connect",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "connect",
				"username":   "foobar",
			},
		},
		{
			uri: "spark://foobar:pass@localhost?api=connect",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "connect",
				"username":   "foobar",
				"password":   "pass",
			},
		},

		{
			uri: "spark://localhost?api=livy&tls=true",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "livy",
				"spark.tls":  "true",
			},
		},
		{
			uri: "spark://localhost?api=livy&tls=true&validateservercertificate=false",
			options: map[string]string{
				"spark.host":                        "localhost",
				"spark.api":                         "livy",
				"spark.tls":                         "true",
				"spark.validate_server_certificate": "false",
			},
		},
		{
			uri: "spark://foobar@localhost:8000?api=livy",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "8000",
				"spark.api":  "livy",
				"username":   "foobar",
			},
		},
		{
			uri: "spark://foobar:pass@localhost:8000?api=livy",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "8000",
				"spark.api":  "livy",
				"username":   "foobar",
				"password":   "pass",
			},
		},

		{
			uri: "spark://localhost?api=thrift%2Bbinary&tls=true",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "thrift+binary",
				"spark.tls":  "true",
			},
		},
		{
			uri: "spark://localhost?api=thrift%2Bbinary&tls=true&validateservercertificate=false",
			options: map[string]string{
				"spark.host":                        "localhost",
				"spark.api":                         "thrift+binary",
				"spark.tls":                         "true",
				"spark.validate_server_certificate": "false",
			},
		},
		{
			uri: "spark://foobar@localhost:8000?api=thrift%2Bbinary",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "8000",
				"spark.api":  "thrift+binary",
				"username":   "foobar",
			},
		},
		{
			uri: "spark://foobar:pass@localhost:8000?api=thrift%2Bbinary",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "8000",
				"spark.api":  "thrift+binary",
				"username":   "foobar",
				"password":   "pass",
			},
		},

		{
			uri: "spark://localhost?api=thrift%2Bhttp&tls=true",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.api":  "thrift+http",
				"spark.tls":  "true",
			},
		},
		{
			uri: "spark://localhost?api=thrift%2Bhttp&tls=true&validateservercertificate=false",
			options: map[string]string{
				"spark.host":                        "localhost",
				"spark.api":                         "thrift+http",
				"spark.tls":                         "true",
				"spark.validate_server_certificate": "false",
			},
		},
		{
			uri: "spark://foobar@localhost:8000?api=thrift%2Bhttp",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "8000",
				"spark.api":  "thrift+http",
				"username":   "foobar",
			},
		},
		{
			uri: "spark://foobar:pass@localhost:8000?api=thrift%2Bhttp",
			options: map[string]string{
				"spark.host": "localhost",
				"spark.port": "8000",
				"spark.api":  "thrift+http",
				"username":   "foobar",
				"password":   "pass",
			},
		},
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		require.NoError(t, err, "failed to parse options from URI %s", tc.uri)
		require.Equal(t, tc.options, options, "unexpected options for URI %s", tc.uri)
	}
}

func TestParseConnectOptionsFromUri(t *testing.T) {
	type testCase struct {
		uri     string
		options connectimpl.ConnectionOpts
	}

	for _, tc := range []testCase{
		{
			uri: "spark://localhost:10000?api=connect&auth_type=none",
			options: connectimpl.ConnectionOpts{
				Host:     "localhost:10000",
				AuthType: connectimpl.AuthTypeNone,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=connect&auth_type=token",
			options: connectimpl.ConnectionOpts{
				Host:     "localhost:10000",
				AuthType: connectimpl.AuthTypeToken,
				Username: "foo",
				Token:    "bar",
			},
		},
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		require.NoError(t, err, "failed to parse options from URI %s", tc.uri)

		parsedOptions, err := connectOptsFromOptions(options)
		require.NoError(t, err, "failed to parse Connect options from URI %s", tc.uri)

		require.Equal(t, tc.options, parsedOptions, "unexpected options for URI %s", tc.uri)
	}
}

func TestParseLivyOptionsFromUri(t *testing.T) {
	type testCase struct {
		uri     string
		options livyimpl.ConnectionOpts
	}

	for _, tc := range []testCase{
		{
			uri: "spark://localhost:10000?api=livy&livy.session_kind=sql",
			options: livyimpl.ConnectionOpts{
				SessionKind:               livyimpl.SessionKindSql,
				AuthType:                  livyimpl.AuthTypeNone,
				BaseURL:                   "http://localhost:10000",
				ValidateServerCertificate: true,
				DeleteSessionOnClose:      true,
			},
		},
		{
			uri: "spark://localhost:10000?api=livy&auth_type=basic&livy.session_kind=sql",
			options: livyimpl.ConnectionOpts{
				SessionKind:               livyimpl.SessionKindSql,
				AuthType:                  livyimpl.AuthTypeBasic,
				BaseURL:                   "http://localhost:10000",
				ValidateServerCertificate: true,
				DeleteSessionOnClose:      true,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=livy&auth_type=basic&livy.session_kind=sql",
			options: livyimpl.ConnectionOpts{
				SessionKind:               livyimpl.SessionKindSql,
				AuthType:                  livyimpl.AuthTypeBasic,
				BaseURL:                   "http://localhost:10000",
				Username:                  "foo",
				Password:                  "bar",
				ValidateServerCertificate: true,
				DeleteSessionOnClose:      true,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=livy&auth_type=basic&livy.session_kind=sql&tls=true",
			options: livyimpl.ConnectionOpts{
				SessionKind:               livyimpl.SessionKindSql,
				AuthType:                  livyimpl.AuthTypeBasic,
				BaseURL:                   "https://localhost:10000",
				Username:                  "foo",
				Password:                  "bar",
				ValidateServerCertificate: true,
				DeleteSessionOnClose:      true,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=livy&auth_type=basic&livy.session_kind=sql&tls=true&validateservercertificate=false",
			options: livyimpl.ConnectionOpts{
				SessionKind:               livyimpl.SessionKindSql,
				AuthType:                  livyimpl.AuthTypeBasic,
				BaseURL:                   "https://localhost:10000",
				Username:                  "foo",
				Password:                  "bar",
				ValidateServerCertificate: false,
				DeleteSessionOnClose:      true,
			},
		},
	} {
		ctx := context.Background()
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		require.NoError(t, err, "failed to parse options from URI %s", tc.uri)

		parsedOptions, err := livyOptsFromOptions(ctx, options)
		require.NoError(t, err, "failed to parse Livy options from URI %s", tc.uri)

		require.Equal(t, tc.options, parsedOptions, "unexpected options for URI %s", tc.uri)
	}
}

func TestParseThriftOptionsFromUri(t *testing.T) {
	type testCase struct {
		uri     string
		options thriftimpl.ConnectionOpts
	}

	for _, tc := range []testCase{
		{
			uri: "spark://localhost:10000?api=thrift%2Bbinary&auth_type=nosasl",
			options: thriftimpl.ConnectionOpts{
				Transport:                 thriftimpl.Binary,
				Auth:                      thriftimpl.NoSasl,
				ValidateServerCertificate: true,
				Host:                      "localhost:10000",
			},
		},
		{
			uri: "spark://localhost:10000?api=thrift%2Bhttp&auth_type=nosasl",
			options: thriftimpl.ConnectionOpts{
				Transport:                 thriftimpl.Http,
				Auth:                      thriftimpl.NoSasl,
				ValidateServerCertificate: true,
				Host:                      "localhost:10000",
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=thrift%2Bhttp&auth_type=plain",
			options: thriftimpl.ConnectionOpts{
				Transport:                 thriftimpl.Http,
				Auth:                      thriftimpl.Plain,
				Username:                  "foo",
				Password:                  "bar",
				ValidateServerCertificate: true,
				Host:                      "localhost:10000",
			},
		},
		{
			uri: "spark://localhost:10000?api=thrift%2Bhttp&auth_type=nosasl&tls=true&validateservercertificate=false",
			options: thriftimpl.ConnectionOpts{
				Transport:                 thriftimpl.Http,
				Auth:                      thriftimpl.NoSasl,
				Tls:                       true,
				ValidateServerCertificate: false,
				Host:                      "localhost:10000",
			},
		},
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		require.NoError(t, err, "failed to parse options from URI %s", tc.uri)

		api := options[sparkutil.OptionApi]
		delete(options, sparkutil.OptionApi)

		parsedOptions, err := thriftOptsFromOptions(api, options)
		require.NoError(t, err, "failed to parse thrift options from URI %s", tc.uri)

		require.Equal(t, tc.options, parsedOptions, "unexpected options for URI %s", tc.uri)
	}
}

func TestParseOptionsFromUriInvalid(t *testing.T) {
	for _, uri := range []string{
		"livy://localhost:10000?api=thrift+binary",
		"sc://localhost:10000?api=thrift+binary",
		"sc://localhost:10000?api=connect&api=connect",
	} {
		u, err := url.Parse(uri)
		require.NoError(t, err)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, "expected error parsing invalid URI %s", uri)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, "expected invalid argument error parsing URI %s", uri)
	}
}
