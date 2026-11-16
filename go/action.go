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
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	pb "google.golang.org/protobuf/proto"
	"io"
	"sync"
	"sync/atomic"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/spark/go/internal/connectimpl"
	proto "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	ActionOption = "adbc.action"
)

type SupportedActions map[string]Action

type ActionState struct {
	supported SupportedActions
	eh        driverbase.ErrorHelper
	current   ActionQuery
}

func (as *ActionState) SetOption(ctx context.Context, key string, value string) (bool, error) {
	if key == ActionOption {
		action, ok := as.supported[value]
		if !ok {
			return true, as.eh.InvalidArgument("unsupported action %s", value)
		}

		if as.current != nil {
			as.current.Close(ctx)
		}

		as.current = action.Init(as.eh)
		return true, nil
	}
	return false, nil
}

func (as *ActionState) Close(ctx context.Context) {
	if as.current != nil {
		as.current.Close(ctx)
		as.current = nil
	}
}

type Action interface {
	Name() string
	Remarks() string
	Init(eh driverbase.ErrorHelper) ActionQuery
}

type ActionQuery interface {
	Close(ctx context.Context)

	Schema() *arrow.Schema
	ParameterSchema() *arrow.Schema

	BindStream(ctx context.Context, params array.RecordReader) error

	ExecuteQuery(ctx context.Context, impl any) (array.RecordReader, int64, error)
	ExecuteUpdate(ctx context.Context, impl any) (int64, error)
}

// ------------------------------------------------------------

// Given a Spark ExecutePlan request, execute it and return the result stream embedded in Arrow.
type executePlanAction struct{}

func (executePlanAction) Name() string    { return "spark.connect.execute_plan" }
func (executePlanAction) Remarks() string { return "Execute a logical plan on the Spark cluster" }
func (executePlanAction) Init(eh driverbase.ErrorHelper) ActionQuery {
	return &executePlanQuery{eh: eh}
}

type executePlanQuery struct {
	eh          driverbase.ErrorHelper
	request     *proto.ExecutePlanRequest
	operationID string
}

func (q *executePlanQuery) Close(ctx context.Context) {}
func (q *executePlanQuery) Schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "response", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}, nil)
}
func (q *executePlanQuery) ParameterSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "request", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}, nil)
}
func (q *executePlanQuery) BindStream(ctx context.Context, params array.RecordReader) error {
	q.request = nil
	q.operationID = ""
	if params == nil || !params.Schema().Equal(q.ParameterSchema()) {
		return q.eh.InvalidArgument("expected one non-null binary request column")
	}
	var request *proto.ExecutePlanRequest
	for params.Next() {
		batch := params.RecordBatch()
		values := batch.Column(0).(*array.Binary)
		for i := range values.Len() {
			if request != nil || values.IsNull(i) {
				return q.eh.InvalidArgument("expected exactly one non-null request")
			}
			request = &proto.ExecutePlanRequest{}
			if err := pb.Unmarshal(values.Value(i), request); err != nil {
				return q.eh.InvalidArgument("invalid ExecutePlan request: %s", err)
			}
		}
	}
	if err := params.Err(); err != nil {
		return err
	}
	if request == nil || request.Plan == nil || request.Plan.OpType == nil {
		return q.eh.InvalidArgument("request requires a plan")
	}
	for _, option := range request.RequestOptions {
		if option.GetReattachOptions().GetReattachable() {
			return q.eh.InvalidArgument("backend reattachment is unsupported")
		}
	}
	q.request = request
	q.operationID = uuid.NewString()
	return nil
}
func (q *executePlanQuery) ExecuteQuery(ctx context.Context, impl any) (array.RecordReader, int64, error) {
	client, err := q.getClient(impl)
	if err != nil {
		return nil, -1, err
	}
	if q.request == nil {
		return nil, -1, q.eh.InvalidState("bind a request before execution")
	}
	request := q.request
	q.request = nil
	request.OperationId = &q.operationID
	_, _, cancel, stream, err := client.ExecutePlanRaw(ctx, request)
	if err != nil {
		return nil, -1, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute plan")
	}
	schema := q.Schema()
	id := q.operationID
	var once sync.Once
	interrupt := func() { once.Do(func() { client.InterruptOperation(id) }) }
	stop := context.AfterFunc(ctx, interrupt)

	// Pull the next response only when the Arrow consumer advances.

	alloc := impl.(*statementImpl).cnxn.Alloc
	rdr := newRelayReader(schema, stream.Recv, alloc)
	rdr.cleanup = func(finished bool) {
		stop()
		if !finished {
			interrupt()
		}
		cancel()
	}
	return rdr, -1, nil
}
func (q *executePlanQuery) ExecuteUpdate(ctx context.Context, impl any) (int64, error) {
	return -1, q.eh.NotImplemented("ExecuteUpdate is unsupported for ExecutePlan actions")
}
func (q *executePlanQuery) getClient(impl any) (*connectimpl.ConnectClient, error) {
	stmt := impl.(*statementImpl)
	client, ok := stmt.cnxn.client.(*connectimpl.ConnectClient)
	if !ok {
		return nil, q.eh.NotImplemented("spark.connect.execute_plan requires Spark Connect client")
	}
	return client, nil
}

var (
	supportedActions SupportedActions = map[string]Action{
		"spark.connect.execute_plan": executePlanAction{},
	}
)

// relayReader uses explicit state: do not use iter.Pull2 across C callbacks.
// Its coroutine retains OS-thread lock state, so consuming or releasing an
// Arrow stream on another thread can fatally terminate the process.
// See https://github.com/golang/go/issues/65889.
type relayReader struct {
	schema  *arrow.Schema
	recv    func() (*proto.ExecutePlanResponse, error)
	alloc   memory.Allocator
	current arrow.RecordBatch
	err     error
	refs    atomic.Int64
	done    bool
	cleanup func(bool)
}

func newRelayReader(schema *arrow.Schema, recv func() (*proto.ExecutePlanResponse, error), alloc memory.Allocator) *relayReader {
	r := &relayReader{schema: schema, recv: recv, alloc: alloc}
	r.refs.Store(1)
	return r
}
func (r *relayReader) finish(finished bool) {
	if r.done {
		return
	}
	r.done = true
	if r.cleanup != nil {
		r.cleanup(finished)
	}
}
func (r *relayReader) Schema() *arrow.Schema { return r.schema }
func (r *relayReader) Retain()               { r.refs.Add(1) }
func (r *relayReader) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		r.finish(false)
	}
}
func (r *relayReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.done {
		return false
	}
	response, err := r.recv()
	if err == io.EOF {
		r.finish(true)
		return false
	}
	if err != nil {
		r.err = sparkbase.ErrToAdbcErr(adbc.StatusIO, connectimpl.NewSparkError(err), "read ExecutePlan response")
		r.finish(false)
		return false
	}
	data, err := pb.Marshal(response)
	if err != nil {
		r.err = sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "read ExecutePlan response")
		r.finish(false)
		return false
	}
	builder := array.NewRecordBuilder(r.alloc, r.schema)
	builder.Field(0).(*array.BinaryBuilder).Append(data)
	r.current = builder.NewRecordBatch()
	builder.Release()
	return true
}
func (r *relayReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *relayReader) Record() arrow.RecordBatch      { return r.current }
func (r *relayReader) Err() error                     { return r.err }
