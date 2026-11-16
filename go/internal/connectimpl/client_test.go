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

package connectimpl

import (
	"context"
	proto "github.com/adbc-drivers/spark/go/internal/generated"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/require"
)

// adbcError asserts that err is an ADBC error and returns it.
func adbcError(t *testing.T, err error) adbc.Error {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	return adbcErr
}

func TestSplitHostPort(t *testing.T) {
	for _, tc := range []struct {
		in   string
		host string
		port int
	}{
		{"localhost", "localhost", defaultPort},
		{"localhost:", "localhost", defaultPort},
		{"localhost:0", "localhost", defaultPort},
		{"localhost:1234", "localhost", 1234},
		{"[::1]:15003", "::1", 15003},
	} {
		host, port, err := splitHostPort(tc.in)
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.host, host, tc.in)
		require.Equal(t, tc.port, port, tc.in)
	}

	_, _, err := splitHostPort("localhost:port")
	require.Equal(t, adbc.StatusInvalidArgument, adbcError(t, err).Code)
}

func TestNewClientRejectsInvalidHostAndPort(t *testing.T) {
	for _, tc := range []struct {
		host string
		msg  string
	}{
		{"", "missing hostname"},
		{":15002", "missing hostname"},
		{"localhost:-1", "port -1 is out of range"},
		{"localhost:65536", "port 65536 is out of range"},
	} {
		client, err := NewClient(context.Background(), ConnectionOpts{Host: tc.host}, nil)
		require.Nil(t, client, tc.host)
		adbcErr := adbcError(t, err)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, tc.host)
		require.Contains(t, adbcErr.Msg, tc.msg, tc.host)
	}
}

func TestUserIDAndClientTypeDefaults(t *testing.T) {
	t.Setenv("USER", "bob")
	require.Equal(t, "bob", userID(""))
	require.Equal(t, "alice", userID("alice"))
	t.Setenv("USER", "")
	require.Equal(t, "na", userID(""))

	require.True(t, strings.HasPrefix(clientType(), "adbc-driver-spark os/"), clientType())
}

func TestPlaintextToken(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		require.Equal(t, []string{"Bearer test-token"}, md.Get("authorization"))
		return &proto.ConfigResponse{}, nil
	}))
	proto.RegisterSparkConnectServiceServer(server, &proto.UnimplementedSparkConnectServiceServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewClient(ctx, ConnectionOpts{Host: listener.Addr().String(), Token: "test-token"}, map[string]string{"test": "value"})
	require.NoError(t, err)
	require.NoError(t, client.Close(ctx))
}
