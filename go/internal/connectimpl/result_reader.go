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
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	proto "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
)

// resultReader streams an ExecutePlan response as Arrow record batches. It
// holds one ArrowBatch at a time and reads the next as the consumer advances.
// Releasing it before the server ends the operation interrupts the operation
// and cancels the RPC.
type resultReader struct {
	refCount atomic.Int64
	schema   *arrow.Schema
	alloc    memory.Allocator
	// ctx is the stream's context, used to tell a cancelled read from a
	// server error.
	ctx       context.Context
	stream    grpc.ServerStreamingClient[proto.ExecutePlanResponse]
	cancel    context.CancelFunc
	interrupt func()

	// buf holds the records of the current ArrowBatch not yet returned.
	buf  []arrow.RecordBatch
	cur  arrow.RecordBatch
	err  error
	done bool
	// finished is set when the server ends the operation, so there is
	// nothing to interrupt. done alone may mean the reader stopped early.
	finished bool
}

var _ array.RecordReader = (*resultReader)(nil)

func newResultReader(ctx context.Context, stream grpc.ServerStreamingClient[proto.ExecutePlanResponse], cancel context.CancelFunc, interrupt func(), alloc memory.Allocator) (*resultReader, error) {
	r := &resultReader{alloc: alloc, ctx: ctx, stream: stream, cancel: cancel, interrupt: interrupt}
	r.refCount.Store(1)
	// Read the first batch so the schema is known and an immediate failure is
	// returned from the execute call.
	r.fill()
	if r.err != nil {
		err := r.err
		r.Release()
		return nil, err
	}
	if r.schema == nil {
		// Commands such as DDL produce no Arrow output.
		r.schema = arrow.NewSchema(nil, nil)
	}
	return r, nil
}

// fill reads responses until it decodes a non-empty ArrowBatch or the stream
// ends.
func (r *resultReader) fill() {
	for !r.done {
		resp, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.done = true
			r.finished = true
			return
		}
		if err != nil {
			r.err = newSparkError(err)
			r.done = true
			// A read that failed because the caller cancelled it or its
			// deadline passed leaves the operation running on the server.
			r.finished = r.ctx.Err() == nil
			return
		}
		data := resp.GetArrowBatch().GetData()
		if len(data) == 0 {
			continue
		}
		if err := r.decode(data); err != nil {
			r.err = err
			r.done = true
			return
		}
		if len(r.buf) > 0 {
			return
		}
	}
}

// decode reads one ArrowBatch, a complete Arrow IPC stream, into buf.
func (r *resultReader) decode(data []byte) error {
	rdr, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(r.alloc))
	if err != nil {
		return fmt.Errorf("decode Arrow batch: %w", err)
	}
	defer rdr.Release()

	if r.schema == nil {
		r.schema = rdr.Schema()
	}
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		r.buf = append(r.buf, rec)
	}
	if err := rdr.Err(); err != nil && !errors.Is(err, io.EOF) {
		r.releaseBuf()
		return fmt.Errorf("read Arrow batch: %w", err)
	}
	return nil
}

func (r *resultReader) releaseBuf() {
	for _, rec := range r.buf {
		rec.Release()
	}
	r.buf = nil
}

func (r *resultReader) Retain() { r.refCount.Add(1) }

func (r *resultReader) Release() {
	if r.refCount.Add(-1) != 0 {
		return
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	r.releaseBuf()
	if !r.finished {
		// Spark keeps running a non-reattachable operation after its RPC is
		// cancelled, so stop it explicitly.
		r.interrupt()
	}
	r.cancel()
}

func (r *resultReader) Schema() *arrow.Schema { return r.schema }

func (r *resultReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if len(r.buf) == 0 {
		r.fill()
	}
	if len(r.buf) == 0 {
		return false
	}
	r.cur = r.buf[0]
	r.buf = r.buf[1:]
	return true
}

func (r *resultReader) RecordBatch() arrow.RecordBatch { return r.cur }

// Deprecated: use RecordBatch.
func (r *resultReader) Record() arrow.RecordBatch { return r.cur }

func (r *resultReader) Err() error {
	if r.err == nil {
		return nil
	}
	return sparkbase.ErrToAdbcErr(adbc.StatusIO, r.err, "read query result")
}
