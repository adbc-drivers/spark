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
	"strings"
	"testing"

	connectpb "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func arrayFromJSON(t *testing.T, dt arrow.DataType, values string) arrow.Array {
	t.Helper()
	arr, _, err := array.FromJSON(memory.DefaultAllocator, dt, strings.NewReader(values))
	require.NoError(t, err)
	t.Cleanup(arr.Release)
	return arr
}

func TestColumnToLiteral(t *testing.T) {
	tzUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	for _, tc := range []struct {
		name string
		arr  arrow.Array
		want *connectpb.Expression_Literal
	}{
		{"null type", arrayFromJSON(t, arrow.Null, `[null]`), nullLiteral()},
		{"boolean", arrayFromJSON(t, arrow.FixedWidthTypes.Boolean, `[true]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Boolean{Boolean: true}}},
		{"int8", arrayFromJSON(t, arrow.PrimitiveTypes.Int8, `[-7]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Byte{Byte: -7}}},
		{"int16", arrayFromJSON(t, arrow.PrimitiveTypes.Int16, `[-300]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Short{Short: -300}}},
		{"int32", arrayFromJSON(t, arrow.PrimitiveTypes.Int32, `[70000]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Integer{Integer: 70000}}},
		{"int64", arrayFromJSON(t, arrow.PrimitiveTypes.Int64, `[9000000000]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Long{Long: 9000000000}}},
		{"uint8", arrayFromJSON(t, arrow.PrimitiveTypes.Uint8, `[255]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Short{Short: 255}}},
		{"uint16", arrayFromJSON(t, arrow.PrimitiveTypes.Uint16, `[65535]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Integer{Integer: 65535}}},
		{"uint32", arrayFromJSON(t, arrow.PrimitiveTypes.Uint32, `[4294967295]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Long{Long: 4294967295}}},
		{"uint64 at int64 max", arrayFromJSON(t, arrow.PrimitiveTypes.Uint64, `[9223372036854775807]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Long{Long: 9223372036854775807}}},
		{"float32", arrayFromJSON(t, arrow.PrimitiveTypes.Float32, `[1.5]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Float{Float: 1.5}}},
		{"float64", arrayFromJSON(t, arrow.PrimitiveTypes.Float64, `[2.25]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Double{Double: 2.25}}},
		{"string", arrayFromJSON(t, arrow.BinaryTypes.String, `["a+b"]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_String_{String_: "a+b"}}},
		{"large string", arrayFromJSON(t, arrow.BinaryTypes.LargeString, `["x"]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_String_{String_: "x"}}},
		{"binary", arrayFromJSON(t, arrow.BinaryTypes.Binary, `["AQI="]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Binary{Binary: []byte{1, 2}}}},
		{"large binary", arrayFromJSON(t, arrow.BinaryTypes.LargeBinary, `["Aw=="]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Binary{Binary: []byte{3}}}},
		{"date32", arrayFromJSON(t, arrow.PrimitiveTypes.Date32, `[19000]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Date{Date: 19000}}},
		{"date64 drops time of day", arrayFromJSON(t, arrow.PrimitiveTypes.Date64, `[172800000]`), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Date{Date: 2}}},
		{"timestamp with zone", timestampArray(t, tzUTC, 1_500_000_123_456), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Timestamp{Timestamp: 1_500_000_123_456}}},
		{"timestamp without zone", timestampArray(t, &arrow.TimestampType{Unit: arrow.Microsecond}, 42), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_TimestampNtz{TimestampNtz: 42}}},
		{"timestamp seconds", timestampArray(t, &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"}, 3), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Timestamp{Timestamp: 3_000_000}}},
		{"timestamp millis", timestampArray(t, &arrow.TimestampType{Unit: arrow.Millisecond}, 5), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_TimestampNtz{TimestampNtz: 5_000}}},
		{"timestamp nanos truncate", timestampArray(t, &arrow.TimestampType{Unit: arrow.Nanosecond}, 1_999), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_TimestampNtz{TimestampNtz: 1}}},
		{"decimal128", arrayFromJSON(t, &arrow.Decimal128Type{Precision: 10, Scale: 2}, `["123.45"]`), decimalLiteral("123.45", 10, 2)},
		{"decimal256", arrayFromJSON(t, &arrow.Decimal256Type{Precision: 40, Scale: 3}, `["-0.125"]`), decimalLiteral("-0.125", 40, 3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lit, err := columnToLiteral(tc.arr, 0)
			require.NoError(t, err)
			require.True(t, proto.Equal(tc.want, lit), "got %v, want %v", lit, tc.want)
		})
	}
}

func timestampArray(t *testing.T, dt *arrow.TimestampType, v int64) arrow.Array {
	t.Helper()
	bldr := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
	defer bldr.Release()
	bldr.Append(arrow.Timestamp(v))
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

func decimalLiteral(value string, precision, scale int32) *connectpb.Expression_Literal {
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Decimal_{Decimal: &connectpb.Expression_Literal_Decimal{
		Value:     value,
		Precision: &precision,
		Scale:     &scale,
	}}}
}

func TestColumnToLiteralNullAndErrors(t *testing.T) {
	// A null of any type binds as Spark's NULL type.
	lit, err := columnToLiteral(arrayFromJSON(t, arrow.PrimitiveTypes.Int64, `[null]`), 0)
	require.NoError(t, err)
	require.True(t, proto.Equal(nullLiteral(), lit))

	// A uint64 above the signed maximum is rejected, not wrapped.
	_, err = columnToLiteral(arrayFromJSON(t, arrow.PrimitiveTypes.Uint64, `[9223372036854775808]`), 0)
	requireAdbcCode(t, err, adbc.StatusInvalidArgument)

	// Types outside the supported set are rejected.
	_, err = columnToLiteral(arrayFromJSON(t, arrow.FixedWidthTypes.Time32s, `[1]`), 0)
	requireAdbcCode(t, err, adbc.StatusNotImplemented)
}

func requireAdbcCode(t *testing.T, err error, code adbc.Status) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, code, adbcErr.Code)
}

func TestCountParameterMarkers(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want int
	}{
		{"SELECT 1", 0},
		{"SELECT ?", 1},
		{"SELECT ?, ?", 2},
		{"SELECT '?'", 0},
		{`SELECT "?" `, 0},
		{"SELECT `a?b`", 0},
		{"SELECT 'it''s ?', ?", 1},
		{`SELECT 'a\'?', ?`, 1},
		{"-- ?\nSELECT ?", 1},
		{"SELECT 1 -- ?", 0},
		{"/* ? */ SELECT ?", 1},
		{"/* outer /* inner ? */ ? */ SELECT ?", 1},
		{"SELECT :named", 0},
		{"SELECT '", 0},
	} {
		require.Equal(t, tc.want, countParameterMarkers(tc.sql), "SQL: %s", tc.sql)
	}
}

// paramsClient records the arguments of each execution.
type paramsClient struct {
	sparkbase.SparkClient
	executions []sparkbase.QueryContext
}

func (c *paramsClient) ExecuteQuery(_ context.Context, q sparkbase.QueryContext) (array.RecordReader, int64, error) {
	c.executions = append(c.executions, q)
	return nil, -1, nil
}

func (c *paramsClient) ExecuteUpdate(_ context.Context, q sparkbase.QueryContext) (int64, error) {
	c.executions = append(c.executions, q)
	return -1, nil
}

func int64Batch(t *testing.T, names []string, values ...[]int64) arrow.RecordBatch {
	t.Helper()
	fields := make([]arrow.Field, len(names))
	cols := make([]arrow.Array, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
		bldr := array.NewInt64Builder(memory.DefaultAllocator)
		bldr.AppendValues(values[i], nil)
		cols[i] = bldr.NewArray()
		bldr.Release()
	}
	rows := int64(len(values[0]))
	rec := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, rows)
	for _, c := range cols {
		c.Release()
	}
	t.Cleanup(rec.Release)
	return rec
}

func newParamsStatement(t *testing.T, sql string) (*statementImpl, *paramsClient) {
	t.Helper()
	client := &paramsClient{}
	st := &statementImpl{cnxn: &connectionImpl{client: client}}
	require.NoError(t, st.SetSqlQuery(context.Background(), sql))
	return st, client
}

func longLiteral(v int64) *connectpb.Expression_Literal {
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Long{Long: v}}
}

func TestExecuteQueryBindsPositionalParameters(t *testing.T) {
	ctx := context.Background()
	st, client := newParamsStatement(t, "SELECT ?, ?")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a", "b"}, []int64{7}, []int64{8})))

	_, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Len(t, client.executions, 1)
	require.Equal(t, "SELECT ?, ?", client.executions[0].Query)
	require.Len(t, client.executions[0].PosArgs, 2)
	require.True(t, proto.Equal(longLiteral(7), client.executions[0].PosArgs[0]))
	require.True(t, proto.Equal(longLiteral(8), client.executions[0].PosArgs[1]))

	// The binding survives execution, so the statement can run again.
	_, _, err = st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Len(t, client.executions, 2)
	require.Len(t, client.executions[1].PosArgs, 2)
	require.True(t, proto.Equal(longLiteral(7), client.executions[1].PosArgs[0]))
}

func TestExecuteUpdateBindsPositionalParameters(t *testing.T) {
	ctx := context.Background()
	st, client := newParamsStatement(t, "INSERT INTO t VALUES (?)")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{5})))

	_, err := st.ExecuteUpdate(ctx)
	require.NoError(t, err)
	require.Len(t, client.executions, 1)
	require.Len(t, client.executions[0].PosArgs, 1)
}

func TestExecuteWithoutMarkersSendsNoArguments(t *testing.T) {
	ctx := context.Background()
	// A query with no markers runs as it did before binding was supported, even
	// with a row bound. The extra values aren't sent.
	st, client := newParamsStatement(t, "SELECT 1")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a", "b"}, []int64{1}, []int64{2})))
	_, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Empty(t, client.executions[0].PosArgs)

	// Multiple rows are fine for a query without markers, too.
	st, client = newParamsStatement(t, "SELECT 1")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{1, 2, 3})))
	_, _, err = st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Empty(t, client.executions[0].PosArgs)
}

func TestExecuteWithMarkersAndNoBindingKeepsSparkError(t *testing.T) {
	// With no binding, the markers reach Spark unchanged, as before.
	st, client := newParamsStatement(t, "SELECT ?")
	_, _, err := st.ExecuteQuery(context.Background())
	require.NoError(t, err)
	require.Empty(t, client.executions[0].PosArgs)
	require.Equal(t, "SELECT ?", client.executions[0].Query)
}

func TestExecuteRejectsParameterMismatches(t *testing.T) {
	ctx := context.Background()

	st, client := newParamsStatement(t, "SELECT ?, ?")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{1})))
	_, _, err := st.ExecuteQuery(ctx)
	requireAdbcCode(t, err, adbc.StatusInvalidArgument)
	require.Empty(t, client.executions, "a mismatched query must not run")

	st, client = newParamsStatement(t, "SELECT ?")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{1, 2})))
	_, _, err = st.ExecuteQuery(ctx)
	requireAdbcCode(t, err, adbc.StatusNotImplemented)
	require.Empty(t, client.executions)

	st, client = newParamsStatement(t, "SELECT ?")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{})))
	_, err = st.ExecuteUpdate(ctx)
	requireAdbcCode(t, err, adbc.StatusInvalidArgument)
	require.Empty(t, client.executions)
}

func TestBindStillAcceptsMultipleRowsForIngest(t *testing.T) {
	// Bulk ingest binds whole tables, so Bind must still accept several rows.
	st, _ := newParamsStatement(t, "SELECT 1")
	require.NoError(t, st.Bind(context.Background(), int64Batch(t, []string{"a"}, []int64{1, 2, 3})))
	require.NotNil(t, st.params)
}

func TestExecuteBindsStreamedRow(t *testing.T) {
	ctx := context.Background()
	st, client := newParamsStatement(t, "SELECT ?")
	rec := int64Batch(t, []string{"a"}, []int64{9})
	stream, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	require.NoError(t, err)
	t.Cleanup(stream.Release)
	require.NoError(t, st.BindStream(ctx, stream))

	_, _, err = st.ExecuteQuery(ctx)
	require.NoError(t, err)
	_, _, err = st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Len(t, client.executions, 2)
	require.True(t, proto.Equal(longLiteral(9), client.executions[1].PosArgs[0]))
}
