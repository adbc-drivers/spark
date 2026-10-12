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
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	proto "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type planStream = grpc.ServerStreamingServer[proto.ExecutePlanResponse]

// fakeServer is an in-process Spark Connect server that records requests.
type fakeServer struct {
	proto.UnimplementedSparkConnectServiceServer

	execute func(req *proto.ExecutePlanRequest, stream planStream) error
	release func(ctx context.Context) error

	mu         sync.Mutex
	plans      []*proto.ExecutePlanRequest
	configs    []*proto.ConfigRequest
	releases   []*proto.ReleaseSessionRequest
	interrupts []*proto.InterruptRequest
	md         metadata.MD
}

func (f *fakeServer) record(ctx context.Context, add func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	add()
	f.md, _ = metadata.FromIncomingContext(ctx)
}

func (f *fakeServer) ExecutePlan(req *proto.ExecutePlanRequest, stream planStream) error {
	f.record(stream.Context(), func() { f.plans = append(f.plans, req) })
	if f.execute == nil {
		return nil
	}
	return f.execute(req, stream)
}

func (f *fakeServer) Config(ctx context.Context, req *proto.ConfigRequest) (*proto.ConfigResponse, error) {
	f.record(ctx, func() { f.configs = append(f.configs, req) })
	return &proto.ConfigResponse{SessionId: req.GetSessionId()}, nil
}

func (f *fakeServer) ReleaseSession(ctx context.Context, req *proto.ReleaseSessionRequest) (*proto.ReleaseSessionResponse, error) {
	f.record(ctx, func() { f.releases = append(f.releases, req) })
	if f.release != nil {
		if err := f.release(ctx); err != nil {
			return nil, err
		}
	}
	return &proto.ReleaseSessionResponse{SessionId: req.GetSessionId()}, nil
}

func (f *fakeServer) Interrupt(ctx context.Context, req *proto.InterruptRequest) (*proto.InterruptResponse, error) {
	f.record(ctx, func() { f.interrupts = append(f.interrupts, req) })
	return &proto.InterruptResponse{SessionId: req.GetSessionId()}, nil
}

func (f *fakeServer) interruptRequests() []*proto.InterruptRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*proto.InterruptRequest(nil), f.interrupts...)
}

func (f *fakeServer) planRequests() []*proto.ExecutePlanRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*proto.ExecutePlanRequest(nil), f.plans...)
}

func (f *fakeServer) configRequests() []*proto.ConfigRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*proto.ConfigRequest(nil), f.configs...)
}

func (f *fakeServer) releaseRequests() []*proto.ReleaseSessionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*proto.ReleaseSessionRequest(nil), f.releases...)
}

func (f *fakeServer) header(key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.md.Get(key)
}

// serve runs fake on a loopback port and returns its address.
func serve(t *testing.T, fake *fakeServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	proto.RegisterSparkConnectServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func connect(t *testing.T, fake *fakeServer, opts ConnectionOpts) *connectClient {
	t.Helper()
	opts.Host = serve(t, fake)
	c, err := newConnectClient(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// arrowBatch encodes rec as one ArrowBatch response and releases it.
func arrowBatch(t *testing.T, rec arrow.RecordBatch) *proto.ExecutePlanResponse {
	t.Helper()
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	return &proto.ExecutePlanResponse{ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
		ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{RowCount: rec.NumRows(), Data: buf.Bytes()},
	}}
}

func int64Batch(values ...int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(values, nil)
	return b.NewRecordBatch()
}

func stringBatch(values ...string) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues(values, nil)
	return b.NewRecordBatch()
}

func sendAll(responses ...*proto.ExecutePlanResponse) func(*proto.ExecutePlanRequest, planStream) error {
	return func(_ *proto.ExecutePlanRequest, stream planStream) error {
		for _, resp := range responses {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
		return nil
	}
}

// sendThenBlock sends responses and then keeps the operation running until
// the client goes away, like a long query.
func sendThenBlock(responses ...*proto.ExecutePlanResponse) func(*proto.ExecutePlanRequest, planStream) error {
	return func(req *proto.ExecutePlanRequest, stream planStream) error {
		if err := sendAll(responses...)(req, stream); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}
}

// requireInterrupted asserts that the operation was interrupted exactly once.
func requireInterrupted(t *testing.T, fake *fakeServer) {
	t.Helper()
	interrupts := fake.interruptRequests()
	require.Len(t, interrupts, 1)
	require.Equal(t, proto.InterruptRequest_INTERRUPT_TYPE_OPERATION_ID, interrupts[0].GetInterruptType())
	require.NotEmpty(t, interrupts[0].GetOperationId())
	require.Equal(t, fake.planRequests()[0].GetOperationId(), interrupts[0].GetOperationId())
}

// adbcError asserts that err is an ADBC error and returns it.
func adbcError(t *testing.T, err error) adbc.Error {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	return adbcErr
}

func TestExecuteQueryStreamsBatches(t *testing.T) {
	fake := &fakeServer{execute: sendAll(
		arrowBatch(t, int64Batch(1, 2)),
		arrowBatch(t, int64Batch(3)),
		arrowBatch(t, int64Batch(4, 5)),
	)}
	c := connect(t, fake, ConnectionOpts{Username: "alice"})

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	rdr, rows, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT id FROM t", Mem: mem})
	require.NoError(t, err)
	require.Equal(t, int64(-1), rows)

	var got []int64
	for rdr.Next() {
		got = append(got, rdr.RecordBatch().Column(0).(*array.Int64).Int64Values()...)
	}
	require.NoError(t, rdr.Err())
	rdr.Release()
	require.Equal(t, []int64{1, 2, 3, 4, 5}, got)

	// One ExecutePlan with a SQL relation root.
	plans := fake.planRequests()
	require.Len(t, plans, 1)
	require.Equal(t, "SELECT id FROM t", plans[0].GetPlan().GetRoot().GetSql().GetQuery())
	require.Equal(t, c.sessionID, plans[0].GetSessionId())
	require.Equal(t, "alice", plans[0].GetUserContext().GetUserId())
	require.True(t, strings.HasPrefix(plans[0].GetClientType(), "adbc-driver-spark "), plans[0].GetClientType())
	require.Empty(t, fake.interruptRequests(), "a fully read result needs no Interrupt")
}

func TestReleaseBeforeTheEndInterruptsTheOperation(t *testing.T) {
	fake := &fakeServer{execute: sendThenBlock(arrowBatch(t, int64Batch(1)))}
	c := connect(t, fake, ConnectionOpts{})

	rdr, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT id FROM range(1000000000)"})
	require.NoError(t, err)
	require.True(t, rdr.Next())
	rdr.Release()

	requireInterrupted(t, fake)
}

func TestCancelledReadInterruptsTheOperation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration // zero cancels the context instead
		want    adbc.Status
	}{
		{"cancel", 0, adbc.StatusCancelled},
		{"deadline", time.Second, adbc.StatusTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeServer{execute: sendThenBlock(arrowBatch(t, int64Batch(1)))}
			c := connect(t, fake, ConnectionOpts{})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.timeout > 0 {
				var cancelTimeout context.CancelFunc
				ctx, cancelTimeout = context.WithTimeout(ctx, tc.timeout)
				defer cancelTimeout()
			}
			rdr, _, err := c.ExecuteQuery(ctx, sparkbase.QueryContext{Query: "SELECT id FROM range(1000000000)"})
			require.NoError(t, err)
			require.True(t, rdr.Next())
			if tc.timeout == 0 {
				cancel()
			}
			// The server is still streaming, so only the client stops the read.
			require.False(t, rdr.Next())
			require.Equal(t, tc.want, adbcError(t, rdr.Err()).Code)
			rdr.Release()

			requireInterrupted(t, fake)
		})
	}
}

func TestDecodeErrorInterruptsTheOperation(t *testing.T) {
	malformed := &proto.ExecutePlanResponse{ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
		ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{RowCount: 1, Data: []byte("not an Arrow IPC stream")},
	}}
	fake := &fakeServer{execute: sendThenBlock(arrowBatch(t, int64Batch(1)), malformed)}
	c := connect(t, fake, ConnectionOpts{})

	rdr, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT id FROM t"})
	require.NoError(t, err)
	require.True(t, rdr.Next())
	require.False(t, rdr.Next())
	require.ErrorContains(t, rdr.Err(), "decode Arrow batch")
	rdr.Release()

	requireInterrupted(t, fake)
}

func TestReleaseCancelsTheStream(t *testing.T) {
	first := arrowBatch(t, int64Batch(1))
	cancelled := make(chan struct{})
	fake := &fakeServer{execute: func(_ *proto.ExecutePlanRequest, stream planStream) error {
		if err := stream.Send(first); err != nil {
			return err
		}
		<-stream.Context().Done()
		close(cancelled)
		return stream.Context().Err()
	}}
	c := connect(t, fake, ConnectionOpts{})

	rdr, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT id FROM range(1000000000)"})
	require.NoError(t, err)
	require.True(t, rdr.Next())
	rdr.Release()

	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("ExecutePlan was not cancelled after Release")
	}
}

func TestReadsRowsLargerThanTheDefaultGRPCLimit(t *testing.T) {
	// Spark writes at least one row per Arrow batch, so one wide row is one message.
	value := strings.Repeat("x", 6<<20)
	fake := &fakeServer{execute: sendAll(arrowBatch(t, stringBatch(value)))}
	c := connect(t, fake, ConnectionOpts{})

	rdr, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT s FROM t"})
	require.NoError(t, err)
	defer rdr.Release()
	require.True(t, rdr.Next())
	require.Len(t, rdr.RecordBatch().Column(0).(*array.String).Value(0), len(value))
}

func TestQueryWithoutArrowOutput(t *testing.T) {
	fake := &fakeServer{}
	c := connect(t, fake, ConnectionOpts{})

	rdr, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "CREATE TABLE t (id INT)"})
	require.NoError(t, err)
	defer rdr.Release()
	require.Equal(t, 0, rdr.Schema().NumFields())
	require.False(t, rdr.Next())
	require.NoError(t, rdr.Err())
}

func TestExecuteUpdateDrainsTheResult(t *testing.T) {
	fake := &fakeServer{execute: sendAll(arrowBatch(t, int64Batch(1)))}
	c := connect(t, fake, ConnectionOpts{})

	n, err := c.ExecuteUpdate(context.Background(), sparkbase.QueryContext{Query: "INSERT INTO t VALUES (1)"})
	require.NoError(t, err)
	require.Equal(t, int64(-1), n)
	require.Len(t, fake.planRequests(), 1)
}

func TestQueryErrorsKeepSparkDetails(t *testing.T) {
	st, err := status.New(codes.Internal,
		"[TABLE_OR_VIEW_NOT_FOUND] The table or view `missing` cannot be found. SQLSTATE: 42P01",
	).WithDetails(&errdetails.ErrorInfo{
		Reason:   "org.apache.spark.sql.catalyst.ExtendedAnalysisException",
		Metadata: map[string]string{"sqlState": "42P01", "errorClass": "TABLE_OR_VIEW_NOT_FOUND"},
	})
	require.NoError(t, err)
	fake := &fakeServer{execute: func(*proto.ExecutePlanRequest, planStream) error { return st.Err() }}
	c := connect(t, fake, ConnectionOpts{})

	_, _, err = c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT * FROM missing"})
	adbcErr := adbcError(t, err)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
	require.Equal(t, [5]byte{'4', '2', 'P', '0', '1'}, adbcErr.SqlState)
	require.Contains(t, adbcErr.Msg, "TABLE_OR_VIEW_NOT_FOUND")
	require.Contains(t, adbcErr.Msg, "missing")
}

func TestQueryErrorsMapGRPCCodes(t *testing.T) {
	fake := &fakeServer{execute: func(*proto.ExecutePlanRequest, planStream) error {
		return status.Error(codes.Unauthenticated, "invalid token")
	}}
	c := connect(t, fake, ConnectionOpts{})

	_, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT 1"})
	adbcErr := adbcError(t, err)
	require.Equal(t, adbc.StatusUnauthenticated, adbcErr.Code)
	require.Contains(t, adbcErr.Msg, "invalid token")
}

func TestErrorAfterTheFirstBatchComesFromTheReader(t *testing.T) {
	first := arrowBatch(t, int64Batch(1))
	fake := &fakeServer{execute: func(_ *proto.ExecutePlanRequest, stream planStream) error {
		if err := stream.Send(first); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "connection reset")
	}}
	c := connect(t, fake, ConnectionOpts{})

	rdr, _, err := c.ExecuteQuery(context.Background(), sparkbase.QueryContext{Query: "SELECT id FROM t"})
	require.NoError(t, err)
	require.True(t, rdr.Next())
	require.False(t, rdr.Next())
	require.Equal(t, adbc.StatusIO, adbcError(t, rdr.Err()).Code)
	rdr.Release()
	require.Empty(t, fake.interruptRequests(), "a failed operation needs no Interrupt")
}

func TestCloseReleasesTheSessionAndTheChannel(t *testing.T) {
	fake := &fakeServer{}
	c := connect(t, fake, ConnectionOpts{SessionID: "session-1", ReleaseSession: true})
	conn := c.conn

	require.NoError(t, c.Close(context.Background()))
	require.Equal(t, connectivity.Shutdown, conn.GetState())
	releases := fake.releaseRequests()
	require.Len(t, releases, 1)
	require.Equal(t, "session-1", releases[0].GetSessionId())
	require.NoError(t, c.Close(context.Background()))
}

func TestCloseKeepsTheSessionWhenReleaseIsDisabled(t *testing.T) {
	fake := &fakeServer{}
	c := connect(t, fake, ConnectionOpts{ReleaseSession: false})
	conn := c.conn

	require.NoError(t, c.Close(context.Background()))
	require.Equal(t, connectivity.Shutdown, conn.GetState())
	require.Empty(t, fake.releaseRequests())
}

func TestCloseIsBoundedWhenTheServerHangs(t *testing.T) {
	timeout := releaseSessionTimeout
	releaseSessionTimeout = 100 * time.Millisecond
	t.Cleanup(func() { releaseSessionTimeout = timeout })

	fake := &fakeServer{release: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	c := connect(t, fake, ConnectionOpts{ReleaseSession: true})
	conn := c.conn

	start := time.Now()
	err := c.Close(context.Background())
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, adbc.StatusTimeout, adbcError(t, err).Code)
	require.Equal(t, connectivity.Shutdown, conn.GetState())
}

func TestNewClientAppliesSessionConfigAndHeaders(t *testing.T) {
	fake := &fakeServer{}
	opts := ConnectionOpts{Host: serve(t, fake), AwsProxyAuth: "proxy-secret"}

	client, err := NewClient(context.Background(), opts, map[string]string{"spark.sql.ansi.enabled": "true"})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close(context.Background())) }()

	configs := fake.configRequests()
	require.Len(t, configs, 1)
	pairs := configs[0].GetOperation().GetSet().GetPairs()
	require.Len(t, pairs, 1)
	require.Equal(t, "spark.sql.ansi.enabled", pairs[0].GetKey())
	require.Equal(t, "true", pairs[0].GetValue())
	require.Equal(t, []string{"proxy-secret"}, fake.header("x-aws-proxy-auth"))
}

func TestTokenIsNeverSentWithoutTLS(t *testing.T) {
	fake := &fakeServer{}
	// A token turns TLS on, so a plaintext server fails the handshake.
	c := connect(t, fake, ConnectionOpts{Token: "secret"})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err := c.ExecuteQuery(ctx, sparkbase.QueryContext{Query: "SELECT 1"})
	require.Error(t, err)
	require.Empty(t, fake.planRequests())
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

	t.Setenv("SPARK_CONNECT_USER_AGENT", "my-app")
	require.True(t, strings.HasPrefix(clientType(), "my-app os/"), clientType())
}
