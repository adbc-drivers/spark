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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuilderParsesTlsOptions(t *testing.T) {
	cb, err := NewBuilder("sc://localhost/;use_ssl=true;validate_server_certificate=false;x-other-header=c")
	require.NoError(t, err)

	require.True(t, cb.useSSL)
	require.False(t, cb.validateServerCertificate)
	require.NotContains(t, cb.Headers(), "use_ssl")
	require.NotContains(t, cb.Headers(), "validate_server_certificate")
	require.Equal(t, "c", cb.Headers()["x-other-header"])
}

func TestBuilderRejectsInvalidTlsOptions(t *testing.T) {
	_, err := NewBuilder("sc://localhost/;use_ssl=maybe")
	require.Error(t, err)

	_, err = NewBuilder("sc://localhost/;validate_server_certificate=maybe")
	require.Error(t, err)
}

func TestBuilderParsesAwsProxyAuthHeader(t *testing.T) {
	cb, err := NewBuilder("sc://localhost/;x-aws-proxy-auth=Bearer+aws+proxy+token")
	require.NoError(t, err)

	require.Equal(t, "Bearer aws proxy token", cb.Headers()["x-aws-proxy-auth"])
	require.Empty(t, cb.Token())
}
