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
	"context"
	connectpb "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"testing"
)

func TestExecutePlanBinding(t *testing.T) {
	q := &executePlanQuery{}
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	plan := &connectpb.Plan{
		OpType: &connectpb.Plan_Root{
			Root: &connectpb.Relation{},
		},
	}
	data, err := proto.Marshal(&connectpb.ExecutePlanRequest{Plan: plan})
	require.NoError(t, err)
	builder := array.NewRecordBuilder(alloc, q.ParameterSchema())
	builder.Field(0).(*array.BinaryBuilder).Append(data)
	record := builder.NewRecordBatch()
	builder.Release()
	reader, err := array.NewRecordReader(record.Schema(), []arrow.RecordBatch{record})
	require.NoError(t, err)
	require.NoError(t, q.BindStream(context.Background(), reader))
	require.NotEmpty(t, q.operationID)
	require.True(t, proto.Equal(q.request.Plan, plan))
	reader.Release()
	record.Release()
}
func TestRelayReaderIsLazy(t *testing.T) {
	calls := 0
	reader := newRelayReader(arrow.NewSchema(nil, nil), func() (*connectpb.ExecutePlanResponse, error) { calls++; return nil, io.EOF }, memory.DefaultAllocator)
	require.Zero(t, calls)
	require.False(t, reader.Next())
	require.Equal(t, 1, calls)
	require.NoError(t, reader.Err())
	reader.Release()
}
