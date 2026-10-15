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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	proto "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type AuthType uint8

const (
	AuthTypeNone AuthType = iota
	AuthTypeToken
)

type ConnectionOpts struct {
	// Host is "hostname" or "hostname:port".
	Host string

	AuthType AuthType // not used
	Username string
	// Token is the OAuth2 bearer token used when AuthType is AuthTypeToken.
	// TLS is enabled when a token is present.
	Token string

	Tls                       bool
	ValidateServerCertificate bool
	AwsProxyAuth              string
	SessionID                 string
	ReleaseSession            bool
}

const (
	defaultPort = 15002
	// gRPC's default limit is 4 MiB, but Spark writes at least one row per
	// Arrow batch, so a single wide row can exceed it. PySpark and the Scala
	// client use 128 MiB.
	maxRecvMsgSize = 128 * 1024 * 1024
	// releaseSessionTimeout bounds Close so an unresponsive server can't hang it.
	releaseSessionTimeout = 5 * time.Second
	// interruptTimeout bounds the Interrupt sent when a reader is released early.
	interruptTimeout = 5 * time.Second
)

type connectClient struct {
	conn           *grpc.ClientConn
	service        proto.SparkConnectServiceClient
	sessionID      string
	userContext    *proto.UserContext
	clientType     string
	headers        metadata.MD
	releaseSession bool
}

func (c *connectClient) BackendName() string {
	return "Spark Connect"
}

// NewClient creates a SparkClient backed by a Spark Connect gRPC session.
func NewClient(ctx context.Context, opts ConnectionOpts, sessionConfig map[string]string) (sparkbase.SparkClient, error) {
	c, err := newConnectClient(opts)
	if err != nil {
		return nil, err
	}
	for k, v := range sessionConfig {
		if err := c.setConfig(ctx, k, v); err != nil {
			_ = c.Close(ctx)
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "set Spark config %s", k)
		}
	}
	return c, nil
}

func newConnectClient(opts ConnectionOpts) (*connectClient, error) {
	host, port, err := splitHostPort(opts.Host)
	if err != nil {
		return nil, err
	}

	dialOpts := []grpc.DialOption{
		grpc.WithAuthority(host),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize)),
	}
	if opts.Tls || opts.Token != "" {
		tlsConfig := &tls.Config{InsecureSkipVerify: !opts.ValidateServerCertificate}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		if opts.Token != "" {
			dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerToken(opts.Token)))
		}
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(net.JoinHostPort(host, strconv.Itoa(port)), dialOpts...)
	if err != nil {
		return nil, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "build Spark Connect channel")
	}

	sessionID := opts.SessionID
	if sessionID == "" {
		sessionID = uuid.NewString()
	}
	headers := metadata.MD{}
	if opts.AwsProxyAuth != "" {
		headers.Set("x-aws-proxy-auth", opts.AwsProxyAuth)
	}
	return &connectClient{
		conn:           conn,
		service:        proto.NewSparkConnectServiceClient(conn),
		sessionID:      sessionID,
		userContext:    &proto.UserContext{UserId: userID(opts.Username)},
		clientType:     clientType(),
		headers:        headers,
		releaseSession: opts.ReleaseSession,
	}, nil
}

// splitHostPort splits "host" or "host:port". As in spark-connect-go, a
// missing or zero port means the default port.
func splitHostPort(hostPort string) (string, int, error) {
	host, port := hostPort, ""
	if strings.Contains(hostPort, ":") {
		var err error
		if host, port, err = net.SplitHostPort(hostPort); err != nil {
			return "", 0, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse Spark Connect host")
		}
	}
	if host == "" {
		return "", 0, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, errors.New("missing hostname"), "parse Spark Connect host")
	}
	portValue := 0
	if port != "" {
		var err error
		if portValue, err = strconv.Atoi(port); err != nil {
			return "", 0, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, err, "parse Spark Connect port")
		}
	}
	if portValue < 0 || portValue > 65535 {
		return "", 0, sparkbase.ErrToAdbcErr(adbc.StatusInvalidArgument, fmt.Errorf("port %d is out of range", portValue), "parse Spark Connect port")
	}
	if portValue == 0 {
		portValue = defaultPort
	}
	return host, portValue, nil
}

// bearerToken sends a token on every RPC. gRPC refuses to send it over a
// connection without transport security.
type bearerToken string

func (t bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

func (bearerToken) RequireTransportSecurity() bool { return true }

// userID defaults to $USER, as spark-connect-go did. Spark scopes server
// sessions by user and session ID.
func userID(username string) string {
	if username != "" {
		return username
	}
	if user := os.Getenv("USER"); user != "" {
		return user
	}
	return "na"
}

// clientType identifies the driver, with spark-connect-go's os/go suffix.
func clientType() string {
	agent := os.Getenv("SPARK_CONNECT_USER_AGENT")
	if agent == "" {
		agent = "adbc-driver-spark"
	}
	return fmt.Sprintf("%s os/%s go/%s", agent, runtime.GOOS, runtime.Version())
}

func (c *connectClient) outgoing(ctx context.Context) context.Context {
	if len(c.headers) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, c.headers)
}

func (c *connectClient) setConfig(ctx context.Context, key, value string) error {
	_, err := c.service.Config(c.outgoing(ctx), &proto.ConfigRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext,
		ClientType:  &c.clientType,
		Operation: &proto.ConfigRequest_Operation{
			OpType: &proto.ConfigRequest_Operation_Set{
				Set: &proto.ConfigRequest_Set{
					Pairs: []*proto.KeyValue{{Key: key, Value: &value}},
				},
			},
		},
	})
	if err != nil {
		return newSparkError(err)
	}
	return nil
}

func (c *connectClient) Close(ctx context.Context) error {
	if c.conn == nil {
		return nil
	}
	var releaseErr error
	if c.releaseSession {
		releaseCtx, cancel := context.WithTimeout(c.outgoing(ctx), releaseSessionTimeout)
		_, releaseErr = c.service.ReleaseSession(releaseCtx, &proto.ReleaseSessionRequest{
			SessionId:   c.sessionID,
			UserContext: c.userContext,
			ClientType:  &c.clientType,
		})
		cancel()
	}
	closeErr := c.conn.Close()
	c.conn = nil
	if releaseErr != nil {
		return sparkbase.ErrToAdbcErr(adbc.StatusIO, newSparkError(releaseErr), "close Spark Connect session")
	}
	if closeErr != nil {
		return sparkbase.ErrToAdbcErr(adbc.StatusIO, closeErr, "close Spark Connect channel")
	}
	return nil
}

// executeSQL runs a query as a single ExecutePlan with a SQL relation root.
func (c *connectClient) executeSQL(ctx context.Context, q sparkbase.QueryContext) (*resultReader, error) {
	plan := &proto.Plan{OpType: &proto.Plan_Root{Root: &proto.Relation{
		RelType: &proto.Relation_Sql{Sql: &proto.SQL{Query: q.Query}},
	}}}
	operationID := uuid.NewString()
	// The stream outlives this call; the reader cancels it on Release.
	streamCtx, cancel := context.WithCancel(c.outgoing(ctx))
	stream, err := c.service.ExecutePlan(streamCtx, &proto.ExecutePlanRequest{
		SessionId:   c.sessionID,
		UserContext: c.userContext,
		ClientType:  &c.clientType,
		OperationId: &operationID,
		Plan:        plan,
	})
	if err != nil {
		cancel()
		return nil, newSparkError(err)
	}
	mem := q.Mem
	if mem == nil {
		mem = memory.DefaultAllocator
	}
	interrupt := func() { c.interruptOperation(operationID) }
	return newResultReader(streamCtx, stream, cancel, interrupt, mem)
}

// interruptOperation stops an operation whose results are no longer wanted.
// Cancelling the RPC alone leaves the job running on the server.
func (c *connectClient) interruptOperation(operationID string) {
	ctx, cancel := context.WithTimeout(c.outgoing(context.Background()), interruptTimeout)
	defer cancel()
	_, _ = c.service.Interrupt(ctx, &proto.InterruptRequest{
		SessionId:     c.sessionID,
		UserContext:   c.userContext,
		ClientType:    &c.clientType,
		InterruptType: proto.InterruptRequest_INTERRUPT_TYPE_OPERATION_ID,
		Interrupt:     &proto.InterruptRequest_OperationId{OperationId: operationID},
	})
}

func (c *connectClient) ExecuteQuery(ctx context.Context, q sparkbase.QueryContext) (array.RecordReader, int64, error) {
	rdr, err := c.executeSQL(ctx, q)
	if err != nil {
		return nil, -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute query")
	}
	// Rows are streamed, so the count isn't known up front.
	return rdr, -1, nil
}

func (c *connectClient) ExecuteUpdate(ctx context.Context, q sparkbase.QueryContext) (int64, error) {
	rdr, err := c.executeSQL(ctx, q)
	if err != nil {
		return -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute update")
	}
	defer rdr.Release()
	for rdr.Next() {
	}
	// Spark Connect does not surface a modified-row count.
	return -1, rdr.Err()
}

func (c *connectClient) VendorVersion(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultVendorVersionImpl(c, ctx, mem)
}

func (c *connectClient) CurrentCatalog(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentCatalogImpl(c, ctx, mem)
}

func (c *connectClient) SetCurrentCatalog(ctx context.Context, mem memory.Allocator, catalog string) error {
	return sparkbase.DefaultSetCurrentCatalogImpl(c, ctx, mem, catalog)
}

func (c *connectClient) CurrentSchema(ctx context.Context, mem memory.Allocator) (string, error) {
	return sparkbase.DefaultCurrentSchemaImpl(c, ctx, mem)
}

func (c *connectClient) SetCurrentSchema(ctx context.Context, mem memory.Allocator, schema string) error {
	return sparkbase.DefaultSetCurrentSchemaImpl(c, ctx, mem, schema)
}

func (c *connectClient) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	return sparkbase.DefaultGetCatalogsImpl(c, ctx, catalogFilter)
}

func (c *connectClient) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	return sparkbase.DefaultGetDBSchemasForCatalogImpl(c, ctx, catalog, schemaFilter)
}

func (c *connectClient) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	return sparkbase.DefaultGetTablesForDBSchemaImpl(c, ctx, catalog, schema, tableFilter, columnFilter, includeColumns)
}

func (c *connectClient) GetOption(_ context.Context, key string) (string, bool, error) {
	switch key {
	case sparkutil.OptionConnectSessionId:
		return c.sessionID, true, nil
	default:
		return "", false, nil
	}
}

func (c *connectClient) GetOptionInt(_ context.Context, _ string) (int64, bool, error) {
	return 0, false, nil
}

var _ sparkbase.SparkClient = (*connectClient)(nil)
