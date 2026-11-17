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
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
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
			uri: "spark://localhost:10000?catalog=analytics",
			options: map[string]string{
				"spark.host":                 "localhost",
				"spark.port":                 "10000",
				"spark.api":                  "thrift+binary",
				adbc.OptionKeyCurrentCatalog: "analytics",
			},
		},
		{
			uri: "sc://localhost:10000",
			options: map[string]string{
				"spark.host":      "localhost",
				"spark.port":      "10000",
				"spark.api":       "connect",
				"spark.auth_type": "none",
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
				"spark.host":      "localhost",
				"spark.api":       "connect",
				"spark.auth_type": "none",
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
				Host:                      "localhost:10000",
				Tls:                       true,
				AuthType:                  connectimpl.AuthTypeNone,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=connect&auth_type=token",
			options: connectimpl.ConnectionOpts{
				Host:                      "localhost:10000",
				Tls:                       true,
				AuthType:                  connectimpl.AuthTypeToken,
				Username:                  "foo",
				Token:                     "bar",
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=connect&auth_type=token&tls=false",
			options: connectimpl.ConnectionOpts{
				Host:                      "localhost:10000",
				Tls:                       false,
				AuthType:                  connectimpl.AuthTypeToken,
				Username:                  "foo",
				Token:                     "bar",
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri: "spark://localhost:10000?api=connect&auth_type=none&tls=true",
			options: connectimpl.ConnectionOpts{
				Host:                      "localhost:10000",
				AuthType:                  connectimpl.AuthTypeNone,
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri: "spark://foo:bar@localhost:10000?api=connect&auth_type=token&tls=true&validateservercertificate=false",
			options: connectimpl.ConnectionOpts{
				Host:                      "localhost:10000",
				AuthType:                  connectimpl.AuthTypeToken,
				Username:                  "foo",
				Token:                     "bar",
				Tls:                       true,
				ValidateServerCertificate: false,
				ReleaseSession:            true,
			},
		},
		{
			uri: "spark://:aws-proxy-token@abc.emr-serverless-services.us-east-1.amazonaws.com:443?api=connect&auth_type=token&tls=true",
			options: connectimpl.ConnectionOpts{
				Host:                      "abc.emr-serverless-services.us-east-1.amazonaws.com:443",
				AuthType:                  connectimpl.AuthTypeToken,
				AwsProxyAuth:              "aws-proxy-token",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri: "spark://localhost:10000?api=connect&auth_type=none&connect.session_id=session-id&connect.release_session=false",
			options: connectimpl.ConnectionOpts{
				Host:                      "localhost:10000",
				Tls:                       true,
				AuthType:                  connectimpl.AuthTypeNone,
				ValidateServerCertificate: true,
				SessionID:                 "session-id",
				ReleaseSession:            false,
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

func TestParseConnectOptionsRejectsInvalidReleaseSession(t *testing.T) {
	u, err := url.Parse("spark://localhost:10000?api=connect&auth_type=none&connect.release_session=invalid")
	require.NoError(t, err)
	options := map[string]string{}
	err = parseOptionsFromUri(u, options)
	require.NoError(t, err)

	_, err = connectOptsFromOptions(options)
	require.Error(t, err)
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

// Existing URI forms must keep their meaning. These pin the current behavior
// next to the Spark Connect parameter tests below.
func TestParseOptionsFromUriExistingForms(t *testing.T) {
	type testCase struct {
		uri     string
		options map[string]string
	}
	for _, tc := range []testCase{
		{
			uri: "sc://localhost:15002/",
			options: map[string]string{
				"spark.host":      "localhost",
				"spark.port":      "15002",
				"spark.api":       "connect",
				"spark.auth_type": "none",
			},
		},
		{
			// A path other than "/;" is still the Livy base URL. For sc, that
			// fails later as an unknown option, as it does today.
			uri: "sc://localhost:15002/foo",
			options: map[string]string{
				"spark.host":          "localhost",
				"spark.port":          "15002",
				"spark.api":           "connect",
				"spark.auth_type":     "none",
				"spark.livy.base_url": "/foo",
			},
		},
		{
			uri: "livy://localhost:10000/gateway",
			options: map[string]string{
				"spark.host":          "localhost",
				"spark.port":          "10000",
				"spark.api":           "livy",
				"spark.livy.base_url": "/gateway",
			},
		},
		{
			// Percent-encoded spaces still decode.
			uri: "sc://localhost:15002?api=connect&connect.session_id=a%20b",
			options: map[string]string{
				"spark.host":               "localhost",
				"spark.port":               "15002",
				"spark.api":                "connect",
				"spark.auth_type":          "none",
				"spark.connect.session_id": "a b",
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

// A literal '+' in a query value is kept, so tokens and base64 secrets survive.
// %2B still decodes to '+'.
func TestParseOptionsFromUriKeepsPlusInQuery(t *testing.T) {
	for _, tc := range []struct {
		uri     string
		session string
	}{
		{"sc://localhost:15002?api=connect&connect.session_id=abc+def/ghi==", "abc+def/ghi=="},
		{"sc://localhost:15002?api=connect&connect.session_id=abc%2Bdef", "abc+def"},
		{"spark://localhost:10000?api=connect&auth_type=none&connect.session_id=a+b%2Bc", "a+b+c"},
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		require.NoError(t, parseOptionsFromUri(u, options), "URI %s", tc.uri)
		require.Equal(t, tc.session, options[sparkutil.OptionConnectSessionId], "URI %s", tc.uri)
	}
}

func TestParseSparkConnectParams(t *testing.T) {
	type testCase struct {
		uri     string
		options map[string]string
		conn    connectimpl.ConnectionOpts
	}
	for _, tc := range []testCase{
		{
			uri: "sc://localhost:15002/;token=abc;use_ssl=true",
			options: map[string]string{
				"spark.host":      "localhost",
				"spark.port":      "15002",
				"spark.api":       "connect",
				"spark.auth_type": "token",
				"password":        "abc",
				"spark.tls":       "true",
			},
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeToken,
				Token:                     "abc",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			// A literal '+' and '/' survive, and %2F and %3B decode to '/' and ';'.
			uri: "sc://localhost/;token=ab+c/d==%2F;x=p%3Bq",
			options: map[string]string{
				"spark.host":                  "localhost",
				"spark.api":                   "connect",
				"spark.auth_type":             "token",
				"password":                    "ab+c/d==/",
				"spark.connect.call_header.x": "p;q",
			},
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost",
				AuthType:                  connectimpl.AuthTypeToken,
				Token:                     "ab+c/d==/",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri: "sc://localhost:15002/;TOKEN=abc;user_id=alice;user_agent=my-app/1.0;session_id=s-1;use_ssl=false;x-trace-id=t1",
			options: map[string]string{
				"spark.host":                           "localhost",
				"spark.port":                           "15002",
				"spark.api":                            "connect",
				"spark.auth_type":                      "token",
				"password":                             "abc",
				"username":                             "alice",
				"spark.connect.user_agent":             "my-app/1.0",
				"spark.connect.session_id":             "s-1",
				"spark.tls":                            "false",
				"spark.connect.call_header.x-trace-id": "t1",
			},
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeToken,
				Username:                  "alice",
				Token:                     "abc",
				UserAgent:                 "my-app/1.0",
				SessionID:                 "s-1",
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			// Repeating a parameter with the same value is harmless.
			uri: "sc://user:abc@localhost:15002/;token=abc;user_id=user;token=abc",
			options: map[string]string{
				"spark.host":      "localhost",
				"spark.port":      "15002",
				"spark.api":       "connect",
				"spark.auth_type": "token",
				"password":        "abc",
				"username":        "user",
			},
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeToken,
				Username:                  "user",
				Token:                     "abc",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			// An empty parameter list adds nothing.
			uri: "sc://localhost:15002/;",
			options: map[string]string{
				"spark.host":      "localhost",
				"spark.port":      "15002",
				"spark.api":       "connect",
				"spark.auth_type": "none",
			},
		},
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		require.NoError(t, parseOptionsFromUri(u, options), "failed to parse URI %s", tc.uri)
		require.Equal(t, tc.options, options, "unexpected options for URI %s", tc.uri)
		if tc.conn.Host == "" {
			continue
		}
		parsed, err := connectOptsFromOptions(options)
		require.NoError(t, err, "failed to parse Connect options from URI %s", tc.uri)
		require.Equal(t, tc.conn, parsed, "unexpected Connect options for URI %s", tc.uri)
	}
}

func TestParseSparkConnectParamsRejectsConflicts(t *testing.T) {
	const secret = "sc-secret-sentinel"
	for _, uri := range []string{
		"sc://localhost/;token=" + secret + ";token=other",
		"sc://localhost/;token=" + secret + "?auth_type=none",
		"sc://localhost/;token=" + secret + "?auth_type=basic",
		"sc://user:other@localhost/;token=" + secret,
		"sc://localhost/;use_ssl=true?tls=false",
		"sc://user:other@localhost/;user_id=alice",
		"sc://localhost/;token=" + secret + ";session_id=s1?connect.session_id=s2",
		"sc://localhost/;x-trace=1;x-trace=2",
		"sc://localhost/;token=" + secret + "?api=thrift+binary",
		"sc://localhost/;token",
	} {
		u, err := url.Parse(uri)
		require.NoError(t, err, "URI %s", uri)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, "expected error parsing URI %s", uri)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, "URI %s", uri)
		require.NotContains(t, err.Error(), secret, "error for URI %s leaks the token", uri)
	}
}

func TestParseSparkConnectParamsRejectsInvalidValues(t *testing.T) {
	const secret = "sc-secret-sentinel"
	// use_ssl must be a boolean. The bad value isn't a secret, but it is
	// reported the same way as other invalid options.
	u, err := url.Parse("sc://localhost/;token=" + secret + ";use_ssl=maybe")
	require.NoError(t, err)
	options := map[string]string{}
	require.NoError(t, parseOptionsFromUri(u, options))
	_, err = connectOptsFromOptions(options)
	require.Error(t, err)

	// A bad escape in a parameter is reported without the value, through the
	// same path a database URI takes.
	options = map[string]string{adbc.OptionKeyURI: "sc://localhost/;token=" + secret + "%zz"}
	err = parseDatabaseURI(options)
	var adbcErr adbc.Error
	require.Error(t, err)
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	require.NotContains(t, err.Error(), secret)

	// Header names must be valid. This check runs when the factory is built.
	u, err = url.Parse("sc://localhost/;token=" + secret + ";bad%20name=x")
	require.NoError(t, err)
	options = map[string]string{}
	require.NoError(t, parseOptionsFromUri(u, options))
	_, err = newSparkClientFactory(context.Background(), options, &sparkbase.CallHeaders{})
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
	require.NotContains(t, err.Error(), secret)
}

// An sc:// URI without an auth_type defaults it: token when the URI has a token
// or a password, and none otherwise. An explicit auth_type always wins.
func TestSparkConnectDefaultAuthType(t *testing.T) {
	for _, tc := range []struct {
		uri      string
		authType string
		conn     connectimpl.ConnectionOpts
	}{
		{
			uri:      "sc://localhost:15002",
			authType: "none",
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeNone,
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri:      "sc://localhost:15002/;use_ssl=false",
			authType: "none",
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeNone,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri:      "sc://:abc@localhost:15002",
			authType: "token",
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeToken,
				Token:                     "abc",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			uri:      "sc://localhost:15002/;token=abc",
			authType: "token",
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeToken,
				Token:                     "abc",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			// A user name alone isn't a token.
			uri:      "sc://alice@localhost:15002",
			authType: "none",
			conn: connectimpl.ConnectionOpts{
				Host:                      "localhost:15002",
				AuthType:                  connectimpl.AuthTypeNone,
				Username:                  "alice",
				Tls:                       true,
				ValidateServerCertificate: true,
				ReleaseSession:            true,
			},
		},
		{
			// An explicit auth_type is kept.
			uri:      "sc://localhost:15002?auth_type=token&password=x",
			authType: "token",
		},
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		require.NoError(t, parseOptionsFromUri(u, options), "URI %s", tc.uri)
		require.Equal(t, tc.authType, options[sparkutil.OptionAuthType], "auth type for %s", tc.uri)
		if tc.conn.Host == "" {
			continue
		}
		parsed, err := connectOptsFromOptions(options)
		require.NoError(t, err, "URI %s", tc.uri)
		require.Equal(t, tc.conn, parsed, "URI %s", tc.uri)
	}
}

func TestSparkConnectExplicitAuthTypeConflicts(t *testing.T) {
	// A password in the userinfo with auth_type=none is a conflict, as before.
	u, err := url.Parse("sc://:abc@localhost:15002?auth_type=none")
	require.NoError(t, err)
	options := map[string]string{}
	require.NoError(t, parseOptionsFromUri(u, options))
	_, err = connectOptsFromOptions(options)
	require.Error(t, err)

	// A token in the path with an explicit non-token auth_type is a conflict too.
	u, err = url.Parse("sc://localhost:15002/;token=abc?auth_type=none")
	require.NoError(t, err)
	options = map[string]string{}
	require.Error(t, parseOptionsFromUri(u, options))
}
