// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adbc-drivers/spark/go/connectclient/client"
	"github.com/adbc-drivers/spark/go/connectclient/client/channel"
	"github.com/adbc-drivers/spark/go/connectclient/client/testutils"
	proto "github.com/adbc-drivers/spark/go/connectclient/internal/generated"
	"github.com/adbc-drivers/spark/go/connectclient/mocks"
	"github.com/adbc-drivers/spark/go/connectclient/sparkerrors"
)

func TestSparkSessionTable(t *testing.T) {
	resetPlanIdForTesting()
	plan := newReadTableRelation("table")
	resetPlanIdForTesting()
	s := testutils.NewConnectServiceClientMock(nil, nil, nil, t)
	c := client.NewSparkExecutorFromClient(s, nil, "")
	session := &sparkSessionImpl{client: c}
	df, err := session.Table("table")
	df_plan := df.(*dataFrameImpl).relation
	assert.Equal(t, plan, df_plan)
	assert.NoError(t, err)
}

func TestSQLCallsExecutePlanWithSQLOnClient(t *testing.T) {
	ctx := context.Background()

	query := "select * from bla"
	// Create the responses:
	responses := []*mocks.MockResponse{
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
					SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{},
				},
			},
			Err: nil,
		},
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_ResultComplete_{
					ResultComplete: &proto.ExecutePlanResponse_ResultComplete{},
				},
			},
			Err: nil,
		},
		{
			Err: io.EOF,
		},
	}

	s := testutils.NewConnectServiceClientMock(&mocks.ProtoClient{
		RecvResponse: responses,
	}, nil, nil, t)
	c := client.NewSparkExecutorFromClient(s, nil, "")

	session := &sparkSessionImpl{
		client: c,
	}
	resp, err := session.Sql(ctx, query)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestNewSessionBuilderCreatesASession(t *testing.T) {
	ctx := context.Background()
	cb, err := channel.NewBuilder(channel.ConnectionParameters{Host: "connection"})
	require.NoError(t, err)
	spark, err := NewSessionBuilder().WithChannelBuilder(cb).Build(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, spark)
}

func TestNewSessionBuilderUsesChannelSessionID(t *testing.T) {
	ctx := context.Background()
	cb, err := channel.NewBuilder(channel.ConnectionParameters{
		Host:      "connection",
		SessionID: "session-id",
	})
	require.NoError(t, err)

	spark, err := NewSessionBuilder().WithChannelBuilder(cb).Build(ctx)
	require.NoError(t, err)
	session, ok := spark.(*sparkSessionImpl)
	require.True(t, ok)
	require.Equal(t, "session-id", session.sessionId)
}

func TestNewSessionBuilderFailsWithoutChannelBuilder(t *testing.T) {
	ctx := context.Background()
	spark, err := NewSessionBuilder().Build(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sparkerrors.InvalidArgumentError)
	assert.Nil(t, spark)
}

func TestSparkSessionStopReleasesSession(t *testing.T) {
	releaseCalled := false
	s := testutils.NewConnectServiceClientMockWithReleaseSession(
		func(ctx context.Context, in *proto.ReleaseSessionRequest) (*proto.ReleaseSessionResponse, error) {
			releaseCalled = true
			require.Equal(t, "session-id", in.GetSessionId())
			return &proto.ReleaseSessionResponse{SessionId: in.GetSessionId()}, nil
		},
		t,
	)
	c := client.NewSparkExecutorFromClient(s, nil, "session-id")
	session := &sparkSessionImpl{
		sessionId:     "session-id",
		client:        c,
		releaseOnStop: true,
	}
	require.NoError(t, session.Stop(context.Background()))
	require.True(t, releaseCalled)
}

func TestSparkSessionStopSkipsReleaseSessionWhenDisabled(t *testing.T) {
	s := testutils.NewConnectServiceClientMock(nil, nil, nil, t)
	c := client.NewSparkExecutorFromClient(s, nil, "session-id")
	session := &sparkSessionImpl{
		sessionId:     "session-id",
		client:        c,
		releaseOnStop: false,
	}

	require.NoError(t, session.Stop(context.Background()))
}

func TestWriteResultStreamsArrowResultToCollector(t *testing.T) {
	ctx := context.Background()

	arrowFields := []arrow.Field{
		{
			Name: "show_string",
			Type: &arrow.StringType{},
		},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	var buf bytes.Buffer
	arrowWriter := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))
	defer func() {
		require.NoError(t, arrowWriter.Close())
	}()

	alloc := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
	defer recordBuilder.Release()

	recordBuilder.Field(0).(*array.StringBuilder).Append("str1a\nstr1b")
	recordBuilder.Field(0).(*array.StringBuilder).Append("str2")

	record := recordBuilder.NewRecordBatch()
	defer record.Release()

	err := arrowWriter.Write(record)
	require.Nil(t, err)

	query := "select * from bla"

	// Create the responses:
	responses := []*mocks.MockResponse{
		// The first stream of response is necessary for the SQL command.
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_SqlCommandResult_{
					SqlCommandResult: &proto.ExecutePlanResponse_SqlCommandResult{},
				},
			},
			Err: nil,
		},
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_ResultComplete_{
					ResultComplete: &proto.ExecutePlanResponse_ResultComplete{},
				},
			},
			Err: nil,
		},
		{
			Err: io.EOF,
		},
		// The second stream of responses is for the actual execution
		{
			Resp: &proto.ExecutePlanResponse{
				ResponseType: &proto.ExecutePlanResponse_ArrowBatch_{
					ArrowBatch: &proto.ExecutePlanResponse_ArrowBatch{
						RowCount: 2,
						Data:     buf.Bytes(),
					},
				},
			},
		},
		{
			Err: io.EOF,
		},
	}

	s := testutils.NewConnectServiceClientMock(&mocks.ProtoClient{
		RecvResponse: responses,
	}, nil, nil, t)
	c := client.NewSparkExecutorFromClient(s, nil, "")

	session := &sparkSessionImpl{
		client: c,
	}

	resp, err := session.Sql(ctx, query)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	df, err := resp.Repartition(ctx, 1, []string{"1"})
	assert.NoError(t, err)
	rows, err := df.Collect(ctx)
	assert.NoError(t, err)
	vals := rows[1].Values()
	assert.NoError(t, err)
	assert.Equal(t, []any{"str2"}, vals)
}
