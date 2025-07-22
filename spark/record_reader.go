// Copyright (c) 2025 Columnar Technologies, Inc.
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
	"fmt"
	"io"

	"github.com/adbc-drivers/apache/spark/internal/hiveserver2"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type recordReaderImpl struct {
	client     *driverbase.Shared[hiveserver2.TCLIServiceClient]
	req        *hiveserver2.TExecuteStatementReq
	handle     *hiveserver2.TOperationHandle
	results    *hiveserver2.TFetchResultsResp
	nextRowIdx int
}

func (rr *recordReaderImpl) AppendRow(builder *array.RecordBuilder) error {
	for rr.results == nil || (rr.results.HasMoreRows != nil && *rr.results.HasMoreRows && rr.nextRowIdx >= len(rr.results.Results.Rows)) {
		var err error
		rr.results, err = driverbase.WithShared(rr.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TFetchResultsResp, error) {
			return client.FetchResults(context.Background(), &hiveserver2.TFetchResultsReq{
				OperationHandle: rr.handle,
				Orientation:     hiveserver2.TFetchOrientation_FETCH_FIRST,
				MaxRows:         65536,
			})
		})
		if err = toAdbcErr(adbc.StatusIO, err, rr.results.Status, "fetch results"); err != nil {
			return err
		}
		rr.nextRowIdx = 0
	}
	if rr.nextRowIdx >= len(rr.results.Results.Rows) {
		rr.results = nil
		return io.EOF
	}
	row := rr.results.Results.Rows[rr.nextRowIdx]
	for i, col := range row.ColVals {
		switch {
		case col.IsSetBoolVal():
			if col.BoolVal.Value == nil {
				builder.Field(i).AppendNull()
			} else {
				builder.Field(i).(*array.BooleanBuilder).Append(*col.BoolVal.Value)
			}

		case col.IsSetI16Val():
			if col.I16Val.Value == nil {
				builder.Field(i).AppendNull()
			} else {
				builder.Field(i).(*array.Int16Builder).Append(*col.I16Val.Value)
			}

		case col.IsSetI32Val():
			if col.I32Val.Value == nil {
				builder.Field(i).AppendNull()
			} else {
				// TODO: i8
				builder.Field(i).(*array.Int32Builder).Append(*col.I32Val.Value)
			}

		case col.IsSetI64Val():
			if col.I64Val.Value == nil {
				builder.Field(i).AppendNull()
			} else {
				builder.Field(i).(*array.Int64Builder).Append(*col.I64Val.Value)
			}

		case col.IsSetStringVal():
			if col.StringVal.Value == nil {
				builder.Field(i).AppendNull()
			} else {
				builder.Field(i).(*array.StringBuilder).Append(*col.StringVal.Value)
			}

		default:
			return adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  fmt.Sprintf("[spark] Unsupported column data %s", col.String()),
			}
		}
	}
	rr.nextRowIdx++
	return nil
}

func (rr *recordReaderImpl) BeginAppending(builder *array.RecordBuilder) error {
	return nil
}

func (rr *recordReaderImpl) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	var schema *arrow.Schema
	resp, err := driverbase.WithShared(rr.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TExecuteStatementResp, error) {
		resp, err := client.ExecuteStatement(ctx, rr.req)
		if err = toAdbcErr(adbc.StatusIO, err, resp.Status, "execute statement"); err != nil {
			return nil, err
		}

		if !resp.OperationHandle.HasResultSet {
			// TODO:
			return nil, errTBD
		}

		meta, err := client.GetResultSetMetadata(ctx, &hiveserver2.TGetResultSetMetadataReq{
			OperationHandle: resp.OperationHandle,
		})
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err, "execute statement")
		} else if err = statusToAdbcErr(meta.Status, "execute statement"); err != nil {
			return nil, err
		}

		fields := make([]arrow.Field, len(meta.Schema.Columns))
		for i, col := range meta.Schema.Columns {
			// Apparently Thrift does not allow recursive trees,
			// so TypeDesc is instead a flattened tree.  Nested
			// types contain indices to their child types

			var ty arrow.DataType
			switch col.TypeDesc.Types[0].PrimitiveEntry.Type {
			case hiveserver2.TTypeId_BOOLEAN_TYPE:
				ty = arrow.FixedWidthTypes.Boolean
			case hiveserver2.TTypeId_TINYINT_TYPE:
				ty = arrow.PrimitiveTypes.Int8
			case hiveserver2.TTypeId_SMALLINT_TYPE:
				ty = arrow.PrimitiveTypes.Int16
			case hiveserver2.TTypeId_INT_TYPE:
				ty = arrow.PrimitiveTypes.Int32
			case hiveserver2.TTypeId_BIGINT_TYPE:
				ty = arrow.PrimitiveTypes.Int64
			case hiveserver2.TTypeId_FLOAT_TYPE:
				ty = arrow.PrimitiveTypes.Float32
			case hiveserver2.TTypeId_DOUBLE_TYPE:
				ty = arrow.PrimitiveTypes.Float64
			case hiveserver2.TTypeId_STRING_TYPE:
				ty = arrow.BinaryTypes.String
			case hiveserver2.TTypeId_VARCHAR_TYPE:
				ty = arrow.BinaryTypes.String
			default:
				return nil, adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg:  fmt.Sprintf("[spark] Unsupported type %s", col.TypeDesc.Types[0].PrimitiveEntry.Type),
				}
			}
			fields[i] = arrow.Field{
				Name:     col.ColumnName,
				Type:     ty,
				Nullable: true,
			}
		}
		schema = arrow.NewSchema(fields, nil)

		return resp, nil
	})

	if err != nil {
		return nil, err
	}
	rr.handle = resp.OperationHandle
	return schema, nil
}

func (rr *recordReaderImpl) Close() error {
	return nil
}

func newRecordReader(ctx context.Context, mem memory.Allocator, client *driverbase.Shared[hiveserver2.TCLIServiceClient], req *hiveserver2.TExecuteStatementReq) (array.RecordReader, error) {
	impl := &recordReaderImpl{
		client: client,
		req:    req,
	}
	rr := &driverbase.BaseRecordReader{}
	err := rr.Init(ctx, mem, nil, 0, impl)
	if err != nil {
		return nil, err
	}
	return rr, nil
}
