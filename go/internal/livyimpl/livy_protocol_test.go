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

// Protocol-level tests against a fake Livy HTTP server, covering the
// behaviors that differ between Apache Livy and Microsoft Fabric's Livy API:
// 202 Accepted session creation, GUID session ids in URLs, explicit statement
// kinds, and session resource defaults.

package livyimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adbc-drivers/spark/go/internal/sparkbase"
)

const fabricGUID = "11111111-2222-3333-4444-555555555555"

type recordedRequest struct {
	method string
	// RequestURI preserves percent-encoding, unlike r.URL.Path.
	uri  string
	body []byte
}

type recordingServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []recordedRequest
}

// newRecordingServer records every request and delegates responses to
// `respond`. A plain HandlerFunc (not a ServeMux) is used so request paths
// are not cleaned or redirected before we can observe them.
func newRecordingServer(t *testing.T, respond func(method, uri string) (int, string)) *recordingServer {
	t.Helper()
	rs := &recordingServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rs.mu.Lock()
		rs.requests = append(rs.requests, recordedRequest{r.Method, r.RequestURI, body})
		rs.mu.Unlock()

		status, payload := respond(r.Method, r.RequestURI)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *recordingServer) find(method, uriPrefix string) *recordedRequest {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for i := range rs.requests {
		if rs.requests[i].method == method && strings.HasPrefix(rs.requests[i].uri, uriPrefix) {
			return &rs.requests[i]
		}
	}
	return nil
}

func newTestClient(baseURL string, sessionConfig map[string]string) *livyClient {
	return &livyClient{
		sessionID:            "",
		sessionConfig:        sessionConfig,
		baseURL:              baseURL,
		httpClient:           &http.Client{Timeout: 10 * time.Second},
		queryTimeout:         time.Minute,
		heartbeatTimeout:     0,
		authType:             AuthTypeNone,
		sessionKind:          SessionKindSql,
		deleteSessionOnClose: true,
	}
}

// statementJSON is a completed statement response shared by tests.
const statementJSON = `{"id":0,"state":"available","output":{"status":"ok","execution_count":0,"data":{}}}`

func TestFabricSessionLifecycle202AndGUIDPaths(t *testing.T) {
	srv := newRecordingServer(t, func(method, uri string) (int, string) {
		switch {
		case method == "POST" && uri == "/sessions":
			// Fabric answers 202 Accepted with a GUID id and no state.
			return http.StatusAccepted, fmt.Sprintf(`{"id":%q,"artifactId":"whatever"}`, fabricGUID)
		case method == "GET" && uri == "/sessions/"+fabricGUID:
			return http.StatusOK, fmt.Sprintf(`{"id":%q,"state":"idle"}`, fabricGUID)
		case method == "POST" && uri == "/sessions/"+fabricGUID+"/statements":
			return http.StatusCreated, statementJSON
		case method == "GET" && uri == "/sessions/"+fabricGUID+"/statements/0":
			return http.StatusOK, statementJSON
		case method == "DELETE" && uri == "/sessions/"+fabricGUID:
			return http.StatusOK, `{}`
		default:
			return http.StatusNotFound, `{"msg":"unexpected request"}`
		}
	})

	c := newTestClient(srv.URL, map[string]string{})
	ctx := context.Background()

	if err := c.openSession(ctx, nil); err != nil {
		t.Fatalf("openSession: %v", err)
	}
	if got := string(c.sessionID); got != fabricGUID {
		t.Fatalf("session id = %q, want %q", got, fabricGUID)
	}

	// The GUID must appear verbatim in session and statement URLs.
	if srv.find("GET", "/sessions/"+fabricGUID) == nil {
		t.Fatal("no session poll GET with GUID path")
	}
	if _, err := c.ExecuteUpdate(ctx, sparkbase.QueryContext{Query: "select 1"}); err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}
	if srv.find("POST", "/sessions/"+fabricGUID+"/statements") == nil {
		t.Fatal("no statement POST with GUID path")
	}
	if srv.find("GET", "/sessions/"+fabricGUID+"/statements/0") == nil {
		t.Fatal("no statement poll GET with GUID path")
	}

	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if srv.find("DELETE", "/sessions/"+fabricGUID) == nil {
		t.Fatal("no session DELETE with GUID path")
	}
}

func TestApacheLivyIntegerSessionCompatibility(t *testing.T) {
	srv := newRecordingServer(t, func(method, uri string) (int, string) {
		switch {
		case method == "POST" && uri == "/sessions":
			// Apache Livy answers 201 Created with an integer id.
			return http.StatusCreated, `{"id":7,"state":"starting"}`
		case method == "GET" && uri == "/sessions/7":
			return http.StatusOK, `{"id":7,"state":"idle"}`
		default:
			return http.StatusNotFound, `{"msg":"unexpected request"}`
		}
	})

	c := newTestClient(srv.URL, map[string]string{})
	if err := c.openSession(context.Background(), nil); err != nil {
		t.Fatalf("openSession: %v", err)
	}
	if got := string(c.sessionID); got != "7" {
		t.Fatalf("session id = %q, want \"7\"", got)
	}
	if srv.find("GET", "/sessions/7") == nil {
		t.Fatal("no session poll GET with integer path")
	}
}

func TestStatementCarriesSessionKind(t *testing.T) {
	srv := newRecordingServer(t, func(method, uri string) (int, string) {
		switch {
		case method == "POST" && uri == "/sessions":
			return http.StatusOK, fmt.Sprintf(`{"id":%q,"state":"idle"}`, fabricGUID)
		case method == "GET" && strings.HasPrefix(uri, "/sessions/"+fabricGUID+"/statements/"):
			return http.StatusOK, statementJSON
		case method == "POST" && strings.HasSuffix(uri, "/statements"):
			return http.StatusCreated, statementJSON
		case method == "GET" && strings.HasPrefix(uri, "/sessions/"):
			return http.StatusOK, fmt.Sprintf(`{"id":%q,"state":"idle"}`, fabricGUID)
		default:
			return http.StatusNotFound, `{"msg":"unexpected request"}`
		}
	})

	c := newTestClient(srv.URL, map[string]string{})
	ctx := context.Background()
	if err := c.openSession(ctx, nil); err != nil {
		t.Fatalf("openSession: %v", err)
	}
	if _, err := c.ExecuteUpdate(ctx, sparkbase.QueryContext{Query: "select 1"}); err != nil {
		t.Fatalf("ExecuteUpdate: %v", err)
	}

	post := srv.find("POST", "/sessions/"+fabricGUID+"/statements")
	if post == nil {
		t.Fatal("no statement POST recorded")
	}
	var payload map[string]any
	if err := json.Unmarshal(post.body, &payload); err != nil {
		t.Fatalf("statement body is not JSON: %v", err)
	}
	if payload["kind"] != "sql" {
		t.Fatalf("statement kind = %v, want \"sql\" (mirrors session kind)", payload["kind"])
	}
	if payload["code"] != "select 1" {
		t.Fatalf("statement code = %v", payload["code"])
	}
}

func TestSessionCreatePayloadResourceDefaults(t *testing.T) {
	sessionCreateBody := func(t *testing.T, sessionConfig map[string]string) map[string]any {
		t.Helper()
		srv := newRecordingServer(t, func(method, uri string) (int, string) {
			switch {
			case method == "POST" && uri == "/sessions":
				return http.StatusOK, `{"id":1,"state":"idle"}`
			case method == "GET" && strings.HasPrefix(uri, "/sessions/"):
				return http.StatusOK, `{"id":1,"state":"idle"}`
			default:
				return http.StatusNotFound, `{}`
			}
		})
		c := newTestClient(srv.URL, sessionConfig)
		if err := c.openSession(context.Background(), nil); err != nil {
			t.Fatalf("openSession: %v", err)
		}
		post := srv.find("POST", "/sessions")
		if post == nil {
			t.Fatal("no session POST recorded")
		}
		var payload map[string]any
		if err := json.Unmarshal(post.body, &payload); err != nil {
			t.Fatalf("session body is not JSON: %v", err)
		}
		return payload
	}

	t.Run("no resource requests by default", func(t *testing.T) {
		payload := sessionCreateBody(t, map[string]string{})
		if _, ok := payload["driverMemory"]; ok {
			t.Fatalf("driverMemory should be omitted by default, got %v", payload["driverMemory"])
		}
		if _, ok := payload["driverCores"]; ok {
			t.Fatalf("driverCores should be omitted by default, got %v", payload["driverCores"])
		}
	})

	t.Run("explicit resources are honored", func(t *testing.T) {
		payload := sessionCreateBody(t, map[string]string{
			"spark.driver.memory":   "4g",
			"spark.driver.cores":    "2",
			"spark.executor.memory": "8g",
			"spark.executor.cores":  "3",
		})
		if payload["driverMemory"] != "4g" {
			t.Fatalf("driverMemory = %v, want 4g", payload["driverMemory"])
		}
		if payload["driverCores"] != float64(2) {
			t.Fatalf("driverCores = %v, want 2", payload["driverCores"])
		}
		if payload["executorMemory"] != "8g" {
			t.Fatalf("executorMemory = %v, want 8g", payload["executorMemory"])
		}
		if payload["executorCores"] != float64(3) {
			t.Fatalf("executorCores = %v, want 3", payload["executorCores"])
		}
	})
}

func TestSessionIDsAreEscapedInURLs(t *testing.T) {
	hostileID := "weird id/../etc"
	escaped := "/sessions/weird%20id%2F..%2Fetc"

	srv := newRecordingServer(t, func(method, uri string) (int, string) {
		if method == "GET" && uri == escaped {
			return http.StatusOK, fmt.Sprintf(`{"id":%q,"state":"idle"}`, hostileID)
		}
		return http.StatusNotFound, `{"msg":"unexpected request"}`
	})

	c := newTestClient(srv.URL, map[string]string{})
	if err := c.openSession(context.Background(), &hostileID); err != nil {
		t.Fatalf("openSession with existing id: %v", err)
	}
	if srv.find("GET", escaped) == nil {
		t.Fatalf("session id was not path-escaped; requests: %+v", srv.requests)
	}
}
