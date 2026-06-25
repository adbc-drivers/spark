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

package channel

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuilderUsesConnectionParameters(t *testing.T) {
	validateServerCertificate := false
	cb, err := NewBuilder(ConnectionParameters{
		Host:                      "localhost",
		Port:                      443,
		Token:                     "token value",
		User:                      "user_id",
		UseSSL:                    true,
		ValidateServerCertificate: &validateServerCertificate,
		Headers: map[string]string{
			"x-other-header": "c",
		},
		SessionID: "session",
		UserAgent: "custom",
	})
	require.NoError(t, err)

	require.Equal(t, "localhost", cb.Host())
	require.Equal(t, 443, cb.Port())
	require.Equal(t, "token value", cb.Token())
	require.Equal(t, "user_id", cb.User())
	require.True(t, cb.useSSL)
	require.False(t, cb.validateServerCertificate)
	require.Equal(t, "c", cb.Headers()["x-other-header"])
	require.Equal(t, "session", cb.SessionId())
	require.True(t, strings.Contains(cb.UserAgent(), "custom"))
	require.True(t, strings.Contains(cb.UserAgent(), "go/"))
	require.True(t, strings.Contains(cb.UserAgent(), "os/"))
}

func TestBuilderAppliesDefaults(t *testing.T) {
	cb, err := NewBuilder(ConnectionParameters{Host: "localhost"})
	require.NoError(t, err)

	require.Equal(t, "localhost", cb.Host())
	require.Equal(t, 15002, cb.Port())
	require.True(t, cb.validateServerCertificate)
	require.Empty(t, cb.Headers())
	require.NotEmpty(t, cb.User())
	_, err = uuid.Parse(cb.SessionId())
	require.NoError(t, err)
	require.True(t, strings.Contains(cb.UserAgent(), "_SPARK_CONNECT_GO"))
	require.True(t, strings.Contains(cb.UserAgent(), "go/"))
	require.True(t, strings.Contains(cb.UserAgent(), "os/"))
}

func TestBuilderRejectsMissingHost(t *testing.T) {
	_, err := NewBuilder(ConnectionParameters{})
	require.Error(t, err)
}

func TestBuilderUsesAwsProxyAuthHeader(t *testing.T) {
	cb, err := NewBuilder(ConnectionParameters{
		Host: "localhost",
		Headers: map[string]string{
			"x-aws-proxy-auth": "Bearer aws proxy token",
		},
	})
	require.NoError(t, err)

	require.Equal(t, "Bearer aws proxy token", cb.Headers()["x-aws-proxy-auth"])
	require.Empty(t, cb.Token())
}
