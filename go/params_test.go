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
	"github.com/apache/arrow-go/v18/arrow/float16"
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
		{"time32 seconds", timeArray(t, arrow.FixedWidthTypes.Time32s, 3723), timeLiteral(3723*1_000_000_000, 0)},
		{"time32 millis", timeArray(t, arrow.FixedWidthTypes.Time32ms, 1500), timeLiteral(1_500_000_000, 3)},
		{"time64 micros", timeArray(t, arrow.FixedWidthTypes.Time64us, 7), timeLiteral(7_000, 6)},
		{"time64 nanos truncate", timeArray(t, arrow.FixedWidthTypes.Time64ns, 1_999), timeLiteral(1_000, 6)},
		{"fixed size binary", fixedBinaryArray(t, 2, []byte{4, 5}), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Binary{Binary: []byte{4, 5}}}},
		{"string view", stringViewArray(t, "view"), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_String_{String_: "view"}}},
		{"binary view", binaryViewArray(t, []byte{9}), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Binary{Binary: []byte{9}}}},
		{"float16", float16Array(t, 1.5), &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Float{Float: 1.5}}},
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
	// A null of a typed column binds as a null of the column's type, so
	// every row of the column has the same type.
	lit, err := columnToLiteral(arrayFromJSON(t, arrow.PrimitiveTypes.Int64, `[null]`), 0)
	require.NoError(t, err)
	require.True(t, proto.Equal(typedNullOf(&connectpb.DataType{Kind: &connectpb.DataType_Long_{Long: &connectpb.DataType_Long{}}}), lit))

	// A uint64 above the signed maximum is rejected, not wrapped.
	_, err = columnToLiteral(arrayFromJSON(t, arrow.PrimitiveTypes.Uint64, `[9223372036854775808]`), 0)
	requireAdbcCode(t, err, adbc.StatusInvalidArgument)

	// Types outside the supported set are rejected.
	_, err = columnToLiteral(arrayFromJSON(t, arrow.ListOf(arrow.PrimitiveTypes.Int64), `[[1]]`), 0)
	requireAdbcCode(t, err, adbc.StatusNotImplemented)
}

func requireAdbcCode(t *testing.T, err error, code adbc.Status) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, code, adbcErr.Code)
}

func typedNullOf(dt *connectpb.DataType) *connectpb.Expression_Literal {
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Null{Null: dt}}
}

func TestTypedNullLiterals(t *testing.T) {
	tzUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	precision := func(p int32) *int32 { return &p }
	for _, tc := range []struct {
		name string
		arr  arrow.Array
		want *connectpb.DataType
	}{
		{"boolean", arrayFromJSON(t, arrow.FixedWidthTypes.Boolean, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Boolean_{Boolean: &connectpb.DataType_Boolean{}}}},
		{"int8", arrayFromJSON(t, arrow.PrimitiveTypes.Int8, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Byte_{Byte: &connectpb.DataType_Byte{}}}},
		{"int16", arrayFromJSON(t, arrow.PrimitiveTypes.Int16, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Short_{Short: &connectpb.DataType_Short{}}}},
		{"int32", arrayFromJSON(t, arrow.PrimitiveTypes.Int32, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Integer_{Integer: &connectpb.DataType_Integer{}}}},
		{"uint64", arrayFromJSON(t, arrow.PrimitiveTypes.Uint64, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Long_{Long: &connectpb.DataType_Long{}}}},
		{"float32", arrayFromJSON(t, arrow.PrimitiveTypes.Float32, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Float_{Float: &connectpb.DataType_Float{}}}},
		{"float64", arrayFromJSON(t, arrow.PrimitiveTypes.Float64, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Double_{Double: &connectpb.DataType_Double{}}}},
		{"string", arrayFromJSON(t, arrow.BinaryTypes.String, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_String_{String_: &connectpb.DataType_String{}}}},
		{"binary", arrayFromJSON(t, arrow.BinaryTypes.Binary, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Binary_{Binary: &connectpb.DataType_Binary{}}}},
		{"date32", arrayFromJSON(t, arrow.PrimitiveTypes.Date32, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Date_{Date: &connectpb.DataType_Date{}}}},
		{"timestamp with zone", arrayFromJSON(t, tzUTC, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Timestamp_{Timestamp: &connectpb.DataType_Timestamp{}}}},
		{"timestamp without zone", arrayFromJSON(t, &arrow.TimestampType{Unit: arrow.Microsecond}, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_TimestampNtz{TimestampNtz: &connectpb.DataType_TimestampNTZ{}}}},
		{"decimal128", arrayFromJSON(t, &arrow.Decimal128Type{Precision: 10, Scale: 2}, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Decimal_{Decimal: &connectpb.DataType_Decimal{Precision: precision(10), Scale: precision(2)}}}},
		{"decimal256", arrayFromJSON(t, &arrow.Decimal256Type{Precision: 40, Scale: 3}, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Decimal_{Decimal: &connectpb.DataType_Decimal{Precision: precision(40), Scale: precision(3)}}}},
		{"time64 micros", arrayFromJSON(t, arrow.FixedWidthTypes.Time64us, `[null]`), &connectpb.DataType{Kind: &connectpb.DataType_Time_{Time: &connectpb.DataType_Time{Precision: precision(6)}}}},
		{"dictionary", dictNullArray(t), &connectpb.DataType{Kind: &connectpb.DataType_String_{String_: &connectpb.DataType_String{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lit, err := columnToLiteral(tc.arr, 0)
			require.NoError(t, err)
			require.True(t, proto.Equal(typedNullOf(tc.want), lit), "got %v, want a null of %v", lit, tc.want)
		})
	}
}

func dictNullArray(t *testing.T) arrow.Array {
	t.Helper()
	bldr := array.NewDictionaryBuilder(memory.DefaultAllocator, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String})
	defer bldr.Release()
	b := bldr.(*array.BinaryDictionaryBuilder)
	b.AppendNull()
	require.NoError(t, b.AppendString("x"))
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
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
		// A raw string has no backslash escapes, so the quote after the
		// backslash closes it, and the marker after it counts.
		{`SELECT r'abc\' AS s, ? AS v`, 1},
		{`SELECT R'abc\' AS s, ? AS v`, 1},
		{`SELECT r"abc\" AS s, ? AS v`, 1},
		{`SELECT R"abc\" AS s, ? AS v`, 1},
		{`SELECT r'a' AS a, r'b\' AS b, ?`, 1},
		// A raw prefix is only a token start: after an identifier, the r is part
		// of the identifier, so the quote starts an ordinary string.
		{`SELECT colr'\', ?`, 0},
		{`SELECT 'r\'', ?`, 1},
	} {
		require.Equal(t, tc.want, countParameterMarkers(tc.sql), "SQL: %s", tc.sql)
	}
}

// paramsClient records the arguments of each execution. Each query returns one
// row holding the first argument's long value, so results can be checked.
type paramsClient struct {
	sparkbase.SparkClient
	executions []sparkbase.QueryContext
}

func (c *paramsClient) ExecuteQuery(_ context.Context, q sparkbase.QueryContext) (array.RecordReader, int64, error) {
	c.executions = append(c.executions, q)
	return echoReader(q.PosArgs), -1, nil
}

func (c *paramsClient) ExecuteUpdate(_ context.Context, q sparkbase.QueryContext) (int64, error) {
	c.executions = append(c.executions, q)
	return -1, nil
}

func echoReader(args []*connectpb.Expression_Literal) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	var v int64
	if len(args) > 0 {
		v = args[0].GetLong()
	}
	bldr.Append(v)
	arr := bldr.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		panic(err)
	}
	return rdr
}

// readValues returns the v column of every row of rdr.
func readValues(t *testing.T, rdr array.RecordReader) []int64 {
	t.Helper()
	defer rdr.Release()
	var values []int64
	for rdr.Next() {
		col := rdr.RecordBatch().Column(0).(*array.Int64)
		for i := range col.Len() {
			values = append(values, col.Value(i))
		}
	}
	require.NoError(t, rdr.Err())
	return values
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
}

func TestExecuteRunsOncePerBoundRow(t *testing.T) {
	ctx := context.Background()

	// ExecuteUpdate runs the query once for each row, in order.
	st, client := newParamsStatement(t, "INSERT INTO t VALUES (?)")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{5, 6, 7})))
	_, err := st.ExecuteUpdate(ctx)
	require.NoError(t, err)
	require.Len(t, client.executions, 3)
	for i, want := range []int64{5, 6, 7} {
		require.True(t, proto.Equal(longLiteral(want), client.executions[i].PosArgs[0]))
	}

	// ExecuteQuery concatenates the results of the rows, in order.
	st, client = newParamsStatement(t, "SELECT 1 + ?")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{4, 5, 6, 7})))
	rdr, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Len(t, client.executions, 1, "only the first row runs before the reader is read")
	require.Equal(t, []int64{4, 5, 6, 7}, readValues(t, rdr))
	require.Len(t, client.executions, 4)
}

func TestExecuteWithNoBoundRowsIsEmpty(t *testing.T) {
	ctx := context.Background()

	st, client := newParamsStatement(t, "INSERT INTO t VALUES (?)")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{})))
	_, err := st.ExecuteUpdate(ctx)
	require.NoError(t, err)
	require.Empty(t, client.executions)

	// A query can't run for a result schema when no row is bound.
	st, client = newParamsStatement(t, "SELECT ?")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"a"}, []int64{})))
	_, _, err = st.ExecuteQuery(ctx)
	requireAdbcCode(t, err, adbc.StatusInvalidArgument)
	require.Empty(t, client.executions)
}

func TestDictionaryValuesBindAsTheirValues(t *testing.T) {
	dict := array.NewDictionaryBuilder(memory.DefaultAllocator, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String})
	defer dict.Release()
	b := dict.(*array.BinaryDictionaryBuilder)
	require.NoError(t, b.AppendString("x"))
	require.NoError(t, b.AppendString("y"))
	require.NoError(t, b.AppendString("x"))
	arr := dict.NewArray()
	t.Cleanup(arr.Release)
	for row, want := range []string{"x", "y", "x"} {
		lit, err := columnToLiteral(arr, row)
		require.NoError(t, err)
		require.True(t, proto.Equal(&connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_String_{String_: want}}, lit))
	}
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

func timeArray(t *testing.T, dt arrow.DataType, v int64) arrow.Array {
	t.Helper()
	bldr := array.NewBuilder(memory.DefaultAllocator, dt)
	defer bldr.Release()
	switch b := bldr.(type) {
	case *array.Time32Builder:
		b.Append(arrow.Time32(v))
	case *array.Time64Builder:
		b.Append(arrow.Time64(v))
	}
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

func timeLiteral(nanos int64, precision int32) *connectpb.Expression_Literal {
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Time_{Time: &connectpb.Expression_Literal_Time{
		Nano:      nanos,
		Precision: &precision,
	}}}
}

func fixedBinaryArray(t *testing.T, width int32, value []byte) arrow.Array {
	t.Helper()
	bldr := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: int(width)})
	defer bldr.Release()
	bldr.Append(value)
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

func stringViewArray(t *testing.T, value string) arrow.Array {
	t.Helper()
	bldr := array.NewStringViewBuilder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.Append(value)
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

func binaryViewArray(t *testing.T, value []byte) arrow.Array {
	t.Helper()
	bldr := array.NewBinaryViewBuilder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.Append(value)
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

func float16Array(t *testing.T, value float32) arrow.Array {
	t.Helper()
	bldr := array.NewFloat16Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.Append(float16.New(value))
	arr := bldr.NewArray()
	t.Cleanup(arr.Release)
	return arr
}

// typedClient answers a query whose result type depends on the first bound
// value: a null gives a null-typed result, and any other value gives int64.
type typedClient struct {
	sparkbase.SparkClient
}

func (c *typedClient) ExecuteQuery(_ context.Context, q sparkbase.QueryContext) (array.RecordReader, int64, error) {
	if len(q.PosArgs) == 1 && q.PosArgs[0].GetNull() != nil {
		return nullTypedReader(), -1, nil
	}
	return echoReader(q.PosArgs), -1, nil
}

func nullTypedReader() array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.Null, Nullable: true}}, nil)
	arr := array.NewNull(1)
	defer arr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		panic(err)
	}
	return rdr
}

func TestExecuteRejectsRowsWithDifferentResultSchemas(t *testing.T) {
	ctx := context.Background()
	st := &statementImpl{cnxn: &connectionImpl{client: &typedClient{}}}
	require.NoError(t, st.SetSqlQuery(ctx, "SELECT ? AS v"))
	require.NoError(t, st.Bind(ctx, nullableInt64Batch(t, `[null, 8]`)))

	rdr, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer rdr.Release()
	// The first row is read with the schema the statement advertised.
	require.True(t, rdr.Next())
	require.False(t, rdr.Next())
	err = rdr.Err()
	requireAdbcCode(t, err, adbc.StatusInvalidArgument)
	require.ErrorContains(t, err, "parameter row 1")
	require.ErrorContains(t, err, "row 0 returned")
}

func TestExecuteWithTypedNullsKeepsResultSchema(t *testing.T) {
	ctx := context.Background()
	st, client := newParamsStatement(t, "SELECT ? AS v")
	require.NoError(t, st.Bind(ctx, int64Batch(t, []string{"v"}, []int64{7, 8})))
	rdr, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.Equal(t, []int64{7, 8}, readValues(t, rdr))
	require.Len(t, client.executions, 2)
}

// nullableInt64Batch is a one-column int64 record with the given JSON values.
func nullableInt64Batch(t *testing.T, values string) arrow.RecordBatch {
	t.Helper()
	col := arrayFromJSON(t, arrow.PrimitiveTypes.Int64, values)
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{col}, int64(col.Len()))
	t.Cleanup(rec.Release)
	return rec
}

// columnReader returns a one-row int64 result with the given nullability.
func columnReader(nullable bool) array.RecordReader {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: nullable}}, nil)
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.Append(1)
	arr := bldr.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		panic(err)
	}
	return rdr
}

func TestRowsReaderAcceptsNullabilityDifferences(t *testing.T) {
	// Row 0 is nullable, as a null row is, and row 1 holds no null.
	opened := 0
	r, err := newRowsReader(func(args []*connectpb.Expression_Literal) (array.RecordReader, error) {
		opened++
		return columnReader(opened == 1), nil
	}, [][]*connectpb.Expression_Literal{{}, {}})
	require.NoError(t, err)
	defer r.Release()
	count := 0
	for r.Next() {
		count++
	}
	require.NoError(t, r.Err())
	require.Equal(t, 2, count)
}
