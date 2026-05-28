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
	"net/url"
	"testing"

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
	} {
		u, err := url.Parse(tc.uri)
		require.NoError(t, err)
		options := map[string]string{}
		err = parseOptionsFromUri(u, options)
		require.NoError(t, err, "failed to parse options from URI %s", tc.uri)
		require.Equal(t, tc.options, options, "unexpected options for URI %s", tc.uri)
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
