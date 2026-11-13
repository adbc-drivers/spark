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

package spark

import (
	"context"
	"net"
	"testing"
	"time"

	proto "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestCallHeaderInheritanceAndRPCs(t *testing.T) {
	type call struct {
		method string
		md     metadata.MD
	}
	calls := make(chan call, 32)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			calls <- call{info.FullMethod, md}
			switch req.(type) {
			case *proto.ConfigRequest:
				return &proto.ConfigResponse{}, nil
			case *proto.ReleaseSessionRequest:
				return &proto.ReleaseSessionResponse{}, nil
			default:
				return handler(ctx, req)
			}
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			md, _ := metadata.FromIncomingContext(stream.Context())
			calls <- call{info.FullMethod, md}
			return nil
		}),
	)
	proto.RegisterSparkConnectServiceServer(server, &proto.UnimplementedSparkConnectServiceServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	check := func(method, value string) {
		t.Helper()
		select {
		case c := <-calls:
			require.Equal(t, "/spark.connect.SparkConnectService/"+method, c.method)
			require.Equal(t, []string{value}, c.md.Get("x-route"))
			require.Equal(t, []string{"trace"}, c.md.Get("x-unrelated"))
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-unrelated", "trace")
	prefix := sparkutil.OptionConnectCallHeaderPrefix
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
		adbc.OptionKeyURI:                          "sc://" + listener.Addr().String() + "?auth_type=none&tls=false",
		prefix + "X-Route":                         "initial",
		sparkutil.OptionSparkConfigPrefix + "test": "value",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()

	dbOpts := db.(adbc.GetSetOptionsWithContext)
	value, err := dbOpts.GetOption(ctx, prefix+"x-ROUTE")
	require.NoError(t, err)
	require.Equal(t, "initial", value)

	c1, err := db.Open(ctx)
	require.NoError(t, err)
	check("Config", "initial")
	c2, err := db.Open(ctx)
	require.NoError(t, err)
	check("Config", "initial")

	opts1 := c1.(adbc.GetSetOptionsWithContext)
	opts2 := c2.(adbc.GetSetOptionsWithContext)
	stmt, err := c1.NewStatement(ctx)
	require.NoError(t, err)
	require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 1"))
	execute := func(expected string) {
		t.Helper()
		_, err := stmt.ExecuteUpdate(ctx)
		require.NoError(t, err)
		check("ExecutePlan", expected)
	}

	execute("initial")
	require.NoError(t, opts1.SetOption(ctx, prefix+"X-Route", "override"))
	execute("override")
	require.NoError(t, dbOpts.SetOption(ctx, prefix+"x-route", "new"))
	execute("override")
	value, err = opts2.GetOption(ctx, prefix+"x-route")
	require.NoError(t, err)
	require.Equal(t, "initial", value)
	c3, err := db.Open(ctx)
	require.NoError(t, err)
	check("Config", "new")
	require.NoError(t, opts1.SetOption(ctx, prefix+"x-route", ""))
	value, err = opts1.GetOption(ctx, prefix+"X-ROUTE")
	require.NoError(t, err)
	require.Empty(t, value)
	execute("")

	_, err = opts1.GetOption(ctx, prefix+"unset")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
	require.Error(t, stmt.(adbc.GetSetOptionsWithContext).SetOption(ctx, prefix+"x-route", "statement"))

	require.NoError(t, stmt.Close(ctx))
	require.NoError(t, c1.Close(ctx))
	check("ReleaseSession", "")
	require.NoError(t, c2.Close(ctx))
	check("ReleaseSession", "initial")
	require.NoError(t, c3.Close(ctx))
	check("ReleaseSession", "new")
}

func TestCallHeadersUnsupported(t *testing.T) {
	ctx := context.Background()
	prefix := sparkutil.OptionConnectCallHeaderPrefix
	for _, api := range []string{"livy", "thrift+binary", "thrift+http"} {
		_, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, map[string]string{
			sparkutil.OptionApi: api, prefix + "x-route": "private",
		})
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
	}
	db := &databaseImpl{}
	require.Error(t, db.SetOption(ctx, prefix+"x-route", "private"))
	_, err := db.GetOption(ctx, prefix+"x-route")
	require.Error(t, err)
	conn := &connectionImpl{}
	require.Error(t, conn.SetOption(ctx, prefix+"x-route", "private"))
	_, err = conn.GetOption(ctx, prefix+"x-route")
	require.Error(t, err)
}

func TestCallHeadersInvalidInitialization(t *testing.T) {
	for key, value := range map[string]string{"bad name": "secret", "x-valid": "secret\n", "x-bin": "secret"} {
		_, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
			adbc.OptionKeyURI: "sc://localhost?auth_type=none&tls=false",
			sparkutil.OptionConnectCallHeaderPrefix + key: value,
		})
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr)
		require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)
		require.NotContains(t, err.Error(), "secret")
	}
}
