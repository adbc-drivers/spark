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

package thriftimpl

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/adbc-drivers/apache/go/internal/hiveserver2"
	"github.com/adbc-drivers/apache/go/internal/sparkbase"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type thriftRecordReader struct {
	client     *driverbase.Shared[hiveserver2.TCLIServiceClient]
	req        *hiveserver2.TExecuteStatementReq
	handle     *hiveserver2.TOperationHandle
	results    *hiveserver2.TFetchResultsResp
	nextRowIdx int
}

func (rr *thriftRecordReader) AppendRow(builder *array.RecordBuilder) error {
	for rr.results == nil || (rr.results.HasMoreRows != nil && *rr.results.HasMoreRows && rr.nextRowIdx >= len(rr.results.Results.Rows)) {
		var err error
		rr.results, err = driverbase.WithShared(rr.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TFetchResultsResp, error) {
			return client.FetchResults(context.Background(), &hiveserver2.TFetchResultsReq{
				OperationHandle: rr.handle,
				Orientation:     hiveserver2.TFetchOrientation_FETCH_FIRST,
				MaxRows:         65536,
			})
		})
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, rr.results, "fetch results"); err != nil {
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
		b := builder.Field(i)
		switch {
		case col.IsSetBoolVal():
			if col.BoolVal.Value == nil {
				b.AppendNull()
			} else {
				b.(*array.BooleanBuilder).Append(*col.BoolVal.Value)
			}

		case col.IsSetByteVal():
			if col.ByteVal.Value == nil {
				b.AppendNull()
			} else {
				b.(*array.Int8Builder).Append(*col.ByteVal.Value)
			}

		case col.IsSetDoubleVal():
			if col.DoubleVal.Value == nil {
				b.AppendNull()
			} else if b.Type().ID() == arrow.FLOAT32 {
				b.(*array.Float32Builder).Append(float32(*col.DoubleVal.Value))
			} else {
				b.(*array.Float64Builder).Append(*col.DoubleVal.Value)
			}

		case col.IsSetI16Val():
			if col.I16Val.Value == nil {
				b.AppendNull()
			} else {
				b.(*array.Int16Builder).Append(*col.I16Val.Value)
			}

		case col.IsSetI32Val():
			if col.I32Val.Value == nil {
				b.AppendNull()
			} else {
				b.(*array.Int32Builder).Append(*col.I32Val.Value)
			}

		case col.IsSetI64Val():
			if col.I64Val.Value == nil {
				b.AppendNull()
			} else {
				b.(*array.Int64Builder).Append(*col.I64Val.Value)
			}

		case col.IsSetStringVal():
			switch {
			case col.StringVal.Value == nil:
				b.AppendNull()
			case b.Type().ID() == arrow.BINARY:
				// TODO(lidavidm): check that the protocol passes binary data correctly
				b.(*array.BinaryBuilder).Append([]byte(*col.StringVal.Value))
			case b.Type().ID() == arrow.DATE32:
				// YYYY-MM-DD
				t, err := time.Parse("2006-01-02", *col.StringVal.Value)
				if err != nil {
					return adbc.Error{
						Code: adbc.StatusInternal,
						Msg:  fmt.Sprintf("[spark] Invalid date value %s: %v", *col.StringVal.Value, err),
					}
				}
				b.(*array.Date32Builder).Append(arrow.Date32FromTime(t))
			case b.Type().ID() == arrow.DECIMAL128:
				ty := b.Type().(*arrow.Decimal128Type)
				d, err := decimal128.FromString(*col.StringVal.Value, ty.Precision, ty.Scale)
				if err != nil {
					return adbc.Error{
						Code: adbc.StatusInternal,
						Msg:  fmt.Sprintf("[spark] Invalid decimal value %s: %v", *col.StringVal.Value, err),
					}
				}
				b.(*array.Decimal128Builder).Append(d)
			case b.Type().ID() == arrow.TIMESTAMP:
				ts, err := arrow.TimestampFromString(*col.StringVal.Value, arrow.Microsecond)
				if err != nil {
					return adbc.Error{
						Code: adbc.StatusInternal,
						Msg:  fmt.Sprintf("[spark] Invalid timestamp value %s: %v", *col.StringVal.Value, err),
					}
				}
				b.(*array.TimestampBuilder).Append(ts)
			default:
				b.(*array.StringBuilder).Append(*col.StringVal.Value)
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

func (rr *thriftRecordReader) BeginAppending(builder *array.RecordBuilder) error {
	return nil
}

func (rr *thriftRecordReader) NextResultSet(ctx context.Context, rec arrow.Record, rowIdx int) (*arrow.Schema, error) {
	var schema *arrow.Schema
	resp, err := driverbase.WithShared(rr.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TExecuteStatementResp, error) {
		resp, err := client.ExecuteStatement(ctx, rr.req)
		if err = sparkbase.ToAdbcErr(adbc.StatusIO, err, resp, "execute statement"); err != nil {
			return nil, err
		}

		if !resp.OperationHandle.HasResultSet {
			// TODO:
			return nil, sparkbase.ErrTBD
		}

		meta, err := client.GetResultSetMetadata(ctx, &hiveserver2.TGetResultSetMetadataReq{
			OperationHandle: resp.OperationHandle,
		})
		if err != nil {
			return nil, sparkbase.ErrToAdbcErr(adbc.StatusIO, err, "execute statement")
		} else if err = sparkbase.StatusToAdbcErr(meta.Status, "execute statement"); err != nil {
			return nil, err
		}

		fields := make([]arrow.Field, len(meta.Schema.Columns))
		for i, col := range meta.Schema.Columns {
			// Apparently Thrift does not allow recursive trees,
			// so TypeDesc is instead a flattened tree.  Nested
			// types contain indices to their child types

			if len(col.TypeDesc.Types) != 1 {
				return nil, adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg:  fmt.Sprintf("[spark] Unsupported type %s", col.TypeDesc),
				}
			}
			desc := col.TypeDesc.Types[0]
			if !desc.IsSetPrimitiveEntry() {
				return nil, adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg:  fmt.Sprintf("[spark] Unsupported type %s", desc),
				}
			}

			var ty arrow.DataType
			switch desc.PrimitiveEntry.Type {
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
			case hiveserver2.TTypeId_TIMESTAMP_TYPE:
				ty = arrow.FixedWidthTypes.Timestamp_us
			case hiveserver2.TTypeId_BINARY_TYPE:
				ty = arrow.BinaryTypes.Binary
			case hiveserver2.TTypeId_DECIMAL_TYPE:
				precision := desc.PrimitiveEntry.TypeQualifiers.Qualifiers[hiveserver2.PRECISION]
				scale := desc.PrimitiveEntry.TypeQualifiers.Qualifiers[hiveserver2.SCALE]
				if precision == nil || scale == nil {
					return nil, adbc.Error{
						Code: adbc.StatusInternal,
						Msg:  fmt.Sprintf("[spark] Decimal type is missing precision/scale: %s", desc),
					}
				}
				ty = &arrow.Decimal128Type{
					Precision: precision.GetI32Value(),
					Scale:     scale.GetI32Value(),
				}
			case hiveserver2.TTypeId_DATE_TYPE:
				ty = arrow.FixedWidthTypes.Date32
			case hiveserver2.TTypeId_VARCHAR_TYPE:
				ty = arrow.BinaryTypes.String
			default:
				return nil, adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg:  fmt.Sprintf("[spark] Unsupported type %s", desc.PrimitiveEntry.Type),
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

func (rr *thriftRecordReader) Close() error {
	return nil
}

func newThriftRecordReader(ctx context.Context, mem memory.Allocator, client *driverbase.Shared[hiveserver2.TCLIServiceClient], req *hiveserver2.TExecuteStatementReq) (array.RecordReader, error) {
	impl := &thriftRecordReader{
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
