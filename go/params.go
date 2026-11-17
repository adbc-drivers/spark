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
	"bytes"
	"fmt"
	"math"
	"strings"

	connectpb "github.com/adbc-drivers/spark/go/internal/generated"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// positionalArgs returns the Spark Connect literals for the parameters bound to
// a SQL query, one per '?' marker. It returns nil when nothing is bound or the
// query has no markers, so queries that only have parameters bound run as they
// did before binding was supported.
//
// The bound records are read and then rebound, so the statement can run again
// with the same binding.
func (st *statementImpl) positionalArgs(query string) ([]*connectpb.Expression_Literal, error) {
	if st.params == nil {
		return nil, nil
	}
	markers := countParameterMarkers(query)
	if markers == 0 {
		return nil, nil
	}

	records, err := st.rebindParams()
	if err != nil {
		return nil, err
	}

	var rows int64
	var row arrow.RecordBatch
	for _, rec := range records {
		rows += rec.NumRows()
		if rec.NumRows() == 1 {
			row = rec
		}
	}
	switch {
	case rows == 0:
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[spark] query has %d parameter markers but no parameter values are bound", markers),
			Code: adbc.StatusInvalidArgument,
		}
	case rows > 1:
		return nil, adbc.Error{
			Msg:  "[spark] binding more than one parameter row is not supported for queries; bind a single row",
			Code: adbc.StatusNotImplemented,
		}
	}
	if int(row.NumCols()) != markers {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[spark] query has %d parameter markers but %d values are bound", markers, row.NumCols()),
			Code: adbc.StatusInvalidArgument,
		}
	}

	literals := make([]*connectpb.Expression_Literal, 0, markers)
	for col := range int(row.NumCols()) {
		lit, err := columnToLiteral(row.Column(col), 0)
		if err != nil {
			return nil, err
		}
		literals = append(literals, lit)
	}
	return literals, nil
}

// rebindParams reads every bound record and binds the same records again, so
// the statement keeps its parameters after they're read. It returns the
// records. They stay valid for as long as the statement's binding does.
func (st *statementImpl) rebindParams() ([]arrow.RecordBatch, error) {
	schema := st.params.Schema()
	var records []arrow.RecordBatch
	for st.params.Next() {
		rec := st.params.RecordBatch()
		rec.Retain()
		records = append(records, rec)
	}
	readErr := st.params.Err()
	st.params.Release()

	rebound, err := array.NewRecordReader(schema, records)
	// The new reader holds its own references, so these are no longer needed.
	for _, rec := range records {
		rec.Release()
	}
	if readErr != nil || err != nil {
		st.params = nil
		if readErr != nil {
			return nil, readErr
		}
		return nil, err
	}
	st.params = rebound
	return records, nil
}

// countParameterMarkers counts the positional '?' markers in a Spark SQL
// statement. Markers inside string literals, quoted identifiers, and comments
// don't count.
func countParameterMarkers(sql string) int {
	count := 0
	for i := 0; i < len(sql); i++ {
		switch c := sql[i]; {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(sql, i)
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = skipBracketedComment(sql, i)
		case c == '?':
			count++
		}
	}
	return count
}

// skipQuoted returns the index of the quote that closes the literal or
// identifier starting at start. Spark's string literals take backslash escapes,
// and a doubled quote doesn't end the literal. An unterminated literal runs to
// the end of the statement.
func skipQuoted(sql string, start int) int {
	quote := sql[start]
	for j := start + 1; j < len(sql); j++ {
		if quote != '`' && sql[j] == '\\' {
			j++
			continue
		}
		if sql[j] == quote {
			return j
		}
	}
	return len(sql) - 1
}

// skipBracketedComment returns the index of the '/' that ends the bracketed
// comment starting at start. Spark allows nested bracketed comments.
func skipBracketedComment(sql string, start int) int {
	depth := 0
	for j := start; j+1 < len(sql); j++ {
		switch {
		case sql[j] == '/' && sql[j+1] == '*':
			depth++
			j++
		case sql[j] == '*' && sql[j+1] == '/':
			depth--
			j++
			if depth == 0 {
				return j
			}
		}
	}
	return len(sql) - 1
}

// columnToLiteral converts the value at row of an Arrow array into a Spark
// Connect SQL literal. Unsupported Arrow types return StatusNotImplemented.
func columnToLiteral(col arrow.Array, row int) (*connectpb.Expression_Literal, error) {
	if col.IsNull(row) {
		return nullLiteral(), nil
	}

	lit := &connectpb.Expression_Literal{}
	switch arr := col.(type) {
	case *array.Null:
		// The null type has no validity bitmap, so IsNull reports false. Every
		// value of this type is null.
		return nullLiteral(), nil
	case *array.Boolean:
		lit.LiteralType = &connectpb.Expression_Literal_Boolean{Boolean: arr.Value(row)}
	case *array.Int8:
		lit.LiteralType = &connectpb.Expression_Literal_Byte{Byte: int32(arr.Value(row))}
	case *array.Int16:
		lit.LiteralType = &connectpb.Expression_Literal_Short{Short: int32(arr.Value(row))}
	case *array.Int32:
		lit.LiteralType = &connectpb.Expression_Literal_Integer{Integer: arr.Value(row)}
	case *array.Int64:
		lit.LiteralType = &connectpb.Expression_Literal_Long{Long: arr.Value(row)}
	case *array.Uint8:
		lit.LiteralType = &connectpb.Expression_Literal_Short{Short: int32(arr.Value(row))}
	case *array.Uint16:
		lit.LiteralType = &connectpb.Expression_Literal_Integer{Integer: int32(arr.Value(row))}
	case *array.Uint32:
		lit.LiteralType = &connectpb.Expression_Literal_Long{Long: int64(arr.Value(row))}
	case *array.Uint64:
		v := arr.Value(row)
		if v > math.MaxInt64 {
			return nil, adbc.Error{
				Msg:  fmt.Sprintf("[spark] uint64 parameter %d exceeds the maximum bind value (Spark long is signed)", v),
				Code: adbc.StatusInvalidArgument,
			}
		}
		lit.LiteralType = &connectpb.Expression_Literal_Long{Long: int64(v)}
	case *array.Float32:
		lit.LiteralType = &connectpb.Expression_Literal_Float{Float: arr.Value(row)}
	case *array.Float64:
		lit.LiteralType = &connectpb.Expression_Literal_Double{Double: arr.Value(row)}
	// String and binary values alias the record's buffers, which are freed when
	// the record is released. Clone them so the literal outlives the binding.
	case *array.String:
		lit.LiteralType = &connectpb.Expression_Literal_String_{String_: strings.Clone(arr.Value(row))}
	case *array.LargeString:
		lit.LiteralType = &connectpb.Expression_Literal_String_{String_: strings.Clone(arr.Value(row))}
	case *array.Binary:
		lit.LiteralType = &connectpb.Expression_Literal_Binary{Binary: bytes.Clone(arr.Value(row))}
	case *array.LargeBinary:
		lit.LiteralType = &connectpb.Expression_Literal_Binary{Binary: bytes.Clone(arr.Value(row))}
	case *array.Date32:
		lit.LiteralType = &connectpb.Expression_Literal_Date{Date: int32(arr.Value(row))}
	case *array.Date64:
		// Date64 counts milliseconds since the epoch, and Spark's date literal
		// counts days. Floor-divide, which drops the time of day.
		const msPerDay = 24 * 60 * 60 * 1000
		lit.LiteralType = &connectpb.Expression_Literal_Date{Date: int32(int64(arr.Value(row)) / msPerDay)}
	case *array.Timestamp:
		tt, ok := arr.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, adbc.Error{Msg: "[spark] timestamp column has no TimestampType", Code: adbc.StatusInternal}
		}
		micros, err := timestampToMicros(int64(arr.Value(row)), tt.Unit)
		if err != nil {
			return nil, err
		}
		// A timestamp with a time zone is an absolute instant, which is Spark's
		// TIMESTAMP. A timestamp without one is wall-clock time, which is
		// TIMESTAMP_NTZ. Both store microseconds since the epoch.
		if tt.TimeZone != "" {
			lit.LiteralType = &connectpb.Expression_Literal_Timestamp{Timestamp: micros}
		} else {
			lit.LiteralType = &connectpb.Expression_Literal_TimestampNtz{TimestampNtz: micros}
		}
	case *array.Decimal128:
		dt := arr.DataType().(*arrow.Decimal128Type)
		prec, scale := dt.Precision, dt.Scale
		lit.LiteralType = &connectpb.Expression_Literal_Decimal_{Decimal: &connectpb.Expression_Literal_Decimal{
			Value:     arr.Value(row).ToString(scale),
			Precision: &prec,
			Scale:     &scale,
		}}
	case *array.Decimal256:
		dt := arr.DataType().(*arrow.Decimal256Type)
		prec, scale := dt.Precision, dt.Scale
		lit.LiteralType = &connectpb.Expression_Literal_Decimal_{Decimal: &connectpb.Expression_Literal_Decimal{
			Value:     arr.Value(row).ToString(scale),
			Precision: &prec,
			Scale:     &scale,
		}}
	default:
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[spark] cannot bind parameter of Arrow type %s", col.DataType()),
			Code: adbc.StatusNotImplemented,
		}
	}
	return lit, nil
}

// nullLiteral returns a null literal of Spark's NULL type. Spark rejects a null
// literal without a type, and the NULL type coerces to the context it's used in.
func nullLiteral() *connectpb.Expression_Literal {
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Null{
		Null: &connectpb.DataType{Kind: &connectpb.DataType_Null{Null: &connectpb.DataType_NULL{}}},
	}}
}

// timestampToMicros converts an Arrow timestamp value to microseconds since the
// epoch, the unit of Spark Connect timestamp literals. Nanoseconds are
// truncated to microseconds, as Spark stores them.
func timestampToMicros(v int64, unit arrow.TimeUnit) (int64, error) {
	switch unit {
	case arrow.Second:
		return v * 1_000_000, nil
	case arrow.Millisecond:
		return v * 1_000, nil
	case arrow.Microsecond:
		return v, nil
	case arrow.Nanosecond:
		return v / 1_000, nil
	default:
		return 0, adbc.Error{
			Msg:  fmt.Sprintf("[spark] unsupported timestamp unit %v", unit),
			Code: adbc.StatusNotImplemented,
		}
	}
}
