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

package livyimpl

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
)

func TestSessionIDUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want SessionID
	}{
		{"apache livy integer id", `{"id":42}`, "42"},
		{"fabric guid id", `{"id":"00000000-1111-2222-3333-444444444444"}`, "00000000-1111-2222-3333-444444444444"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s Session
			if err := json.Unmarshal([]byte(tc.in), &s); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if s.ID != tc.want {
				t.Fatalf("got %q, want %q", s.ID, tc.want)
			}
		})
	}

	var s Session
	if err := json.Unmarshal([]byte(`{"id":{"nested":true}}`), &s); err == nil {
		t.Fatal("expected error for non-scalar session id")
	}
}

func TestAzureTokenScope(t *testing.T) {
	cases := []struct {
		name     string
		baseURL  string
		override string
		want     string
	}{
		{
			"fabric host infers fabric scope",
			"https://api.fabric.microsoft.com/v1/workspaces/w/lakehouses/l/livyapi/versions/2023-12-01",
			"",
			"https://api.fabric.microsoft.com/.default",
		},
		{
			"synapse host keeps synapse scope",
			"https://myworkspace.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/pool",
			"",
			"https://dev.azuresynapse.net/",
		},
		{
			"override always wins",
			"https://api.fabric.microsoft.com/v1/workspaces/w/lakehouses/l/livyapi/versions/2023-12-01",
			"https://example.com/.default",
			"https://example.com/.default",
		},
		{
			"fabric substring in path does not trigger fabric scope",
			"https://example.com/fabric.microsoft.com",
			"",
			"https://dev.azuresynapse.net/",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := azureTokenScope(tc.baseURL, tc.override); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNewAzureCredentialValidation(t *testing.T) {
	t.Run("service principal requires tenant, client id and secret", func(t *testing.T) {
		_, err := newAzureCredential(ConnectionOpts{
			AzureCredential: sparkutil.OptionValueAzureCredentialServicePrincipal,
			AzureTenantID:   "tenant",
		})
		var adbcErr adbc.Error
		if err == nil {
			t.Fatal("expected error")
		}
		if !errorAs(err, &adbcErr) || adbcErr.Code != adbc.StatusInvalidArgument {
			t.Fatalf("expected InvalidArgument adbc.Error, got %v", err)
		}
		if !strings.Contains(err.Error(), sparkutil.OptionLivyAzureClientSecret) {
			t.Fatalf("error should name the missing options: %v", err)
		}
	})

	t.Run("invalid credential kind is rejected", func(t *testing.T) {
		_, err := newAzureCredential(ConnectionOpts{AzureCredential: "carrier_pigeon"})
		if err == nil || !strings.Contains(err.Error(), "carrier_pigeon") {
			t.Fatalf("expected invalid-credential error, got %v", err)
		}
	})

	t.Run("service principal with all fields constructs", func(t *testing.T) {
		cred, err := newAzureCredential(ConnectionOpts{
			AzureCredential:   sparkutil.OptionValueAzureCredentialServicePrincipal,
			AzureTenantID:     "11111111-1111-1111-1111-111111111111",
			AzureClientID:     "22222222-2222-2222-2222-222222222222",
			AzureClientSecret: "hunter2",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cred == nil {
			t.Fatal("expected credential")
		}
	})
}

func errorAs(err error, target *adbc.Error) bool {
	e, ok := err.(adbc.Error)
	if ok {
		*target = e
	}
	return ok
}
