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

// positionalRows returns the Spark Connect literals of each bound row, one list
// per row, for a SQL query with '?' markers. The query runs once per row, as
// ADBC binds it. It returns nil when nothing is bound or the query has no
// markers, so the query runs once with no arguments, as it did before binding
// was supported. A binding with no rows returns an empty, non-nil slice.
//
// The bound records are read and then rebound, so the statement can run again
// with the same binding.
func (st *statementImpl) positionalRows(query string) ([][]*connectpb.Expression_Literal, error) {
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

	rows := make([][]*connectpb.Expression_Literal, 0)
	for _, rec := range records {
		if int(rec.NumCols()) != markers {
			if rec.NumRows() == 0 {
				continue
			}
			return nil, adbc.Error{
				Msg:  fmt.Sprintf("[spark] query has %d parameter markers but %d values are bound", markers, rec.NumCols()),
				Code: adbc.StatusInvalidArgument,
			}
		}
		for row := range int(rec.NumRows()) {
			literals := make([]*connectpb.Expression_Literal, 0, markers)
			for col := range int(rec.NumCols()) {
				lit, err := columnToLiteral(rec.Column(col), row)
				if err != nil {
					return nil, err
				}
				literals = append(literals, lit)
			}
			rows = append(rows, literals)
		}
	}
	return rows, nil
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
// statement. It skips every construct that hides a '?' in Spark's lexer
// (SqlBaseLexer.g4): string literals, raw strings, quoted identifiers,
// dollar-quoted strings, and comments. A hint, /*+ ... */, is not a comment,
// so its contents are tokens and their markers count.
func countParameterMarkers(sql string) int {
	count := 0
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		next := byte(0)
		if i+1 < len(sql) {
			next = sql[i+1]
		}
		switch {
		case c == '-' && next == '-':
			i = skipLineComment(sql, i)
		case c == '/' && next == '*' && i+2 < len(sql) && sql[i+2] == '+':
			// A hint opens with /*+. Its contents are ordinary tokens.
			i += 2
		case c == '/' && next == '*':
			i = skipBracketedComment(sql, i)
		case (c == 'r' || c == 'R') && isQuote(next) && !afterIdentifier(sql, i):
			// A raw string, R'...' or R"...", has no backslash escapes.
			i = skipRaw(sql, i+1)
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(sql, i)
		case c == '$':
			if end, ok := skipDollarQuoted(sql, i); ok {
				i = end
			}
		case c == '?':
			count++
		}
	}
	return count
}

func isQuote(c byte) bool {
	return c == '\'' || c == '"'
}

// afterIdentifier reports whether the byte before position i could continue an
// identifier, in which case the character at i is part of that identifier and
// not a string prefix. Spark's lexer matches identifiers greedily, so
// colr'...' is an identifier followed by a string. An identifier is letters,
// digits and underscores; a byte of 0x80 or more is part of a Unicode letter.
// '$' is never part of an identifier.
func afterIdentifier(sql string, i int) bool {
	if i == 0 {
		return false
	}
	c := sql[i-1]
	return c == '_' || c >= 0x80 ||
		('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// skipLineComment returns the index of the last byte of the -- comment that
// starts at start. Spark's comment continues past a backslash that ends a
// line, so a '?' on the next line is part of the comment.
func skipLineComment(sql string, start int) int {
	for j := start + 2; j < len(sql); j++ {
		switch {
		case sql[j] == '\\' && j+1 < len(sql) && sql[j+1] == '\n':
			j++
		case sql[j] == '\r' || sql[j] == '\n':
			return j - 1
		}
	}
	return len(sql) - 1
}

// skipRaw returns the index of the quote that closes the raw string whose
// opening quote is at start. A raw string has no escapes, so the first matching
// quote closes it. An unterminated string runs to the end of the statement.
func skipRaw(sql string, start int) int {
	quote := sql[start]
	for j := start + 1; j < len(sql); j++ {
		if sql[j] == quote {
			return j
		}
	}
	return len(sql) - 1
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

// skipDollarQuoted handles a dollar-quoted string, $tag$ ... $tag$, where tag is
// letters and may be empty. It reports the index of the last byte of the string
// and whether sql has one at i. The string ends at the first occurrence of the
// same tag text, compared case-sensitively, as Spark's lexer does. A '$' that
// doesn't open a tag, such as $1, is not a string. An unterminated string runs
// to the end of the statement.
func skipDollarQuoted(sql string, start int) (int, bool) {
	j := start + 1
	for j < len(sql) && isASCIILetter(sql[j]) {
		j++
	}
	if j >= len(sql) || sql[j] != '$' {
		return 0, false
	}
	tag := sql[start : j+1]
	body := j + 1
	if k := strings.Index(sql[body:], tag); k >= 0 {
		return body + k + len(tag) - 1, true
	}
	return len(sql) - 1, true
}

func isASCIILetter(c byte) bool {
	return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

// skipBracketedComment returns the index of the '/' that ends the bracketed
// comment starting at start. Comments nest. A '/*+' inside a comment is text,
// not a nested comment, because a hint is never a comment.
func skipBracketedComment(sql string, start int) int {
	depth := 0
	for j := start; j+1 < len(sql); j++ {
		switch {
		case sql[j] == '/' && sql[j+1] == '*' && (j+2 >= len(sql) || sql[j+2] != '+'):
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
		return typedNullLiteral(col.DataType())
	}

	lit := &connectpb.Expression_Literal{}
	switch arr := col.(type) {
	case *array.Dictionary:
		// A dictionary-encoded value binds as the value it refers to.
		return columnToLiteral(arr.Dictionary(), arr.GetValueIndex(row))
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
	case *array.Float16:
		// Spark has no half-precision type, so a half float binds as a float.
		lit.LiteralType = &connectpb.Expression_Literal_Float{Float: arr.Value(row).Float32()}
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
	case *array.FixedSizeBinary:
		lit.LiteralType = &connectpb.Expression_Literal_Binary{Binary: bytes.Clone(arr.Value(row))}
	case *array.StringView:
		lit.LiteralType = &connectpb.Expression_Literal_String_{String_: strings.Clone(arr.Value(row))}
	case *array.BinaryView:
		lit.LiteralType = &connectpb.Expression_Literal_Binary{Binary: bytes.Clone(arr.Value(row))}
	case *array.Time32:
		tt, ok := arr.DataType().(*arrow.Time32Type)
		if !ok {
			return nil, adbc.Error{Msg: "[spark] time column has no Time32Type", Code: adbc.StatusInternal}
		}
		nanos, precision, err := timeToNanos(int64(arr.Value(row)), tt.Unit)
		if err != nil {
			return nil, err
		}
		lit.LiteralType = &connectpb.Expression_Literal_Time_{Time: &connectpb.Expression_Literal_Time{
			Nano:      nanos,
			Precision: &precision,
		}}
	case *array.Time64:
		tt, ok := arr.DataType().(*arrow.Time64Type)
		if !ok {
			return nil, adbc.Error{Msg: "[spark] time column has no Time64Type", Code: adbc.StatusInternal}
		}
		nanos, precision, err := timeToNanos(int64(arr.Value(row)), tt.Unit)
		if err != nil {
			return nil, err
		}
		lit.LiteralType = &connectpb.Expression_Literal_Time_{Time: &connectpb.Expression_Literal_Time{
			Nano:      nanos,
			Precision: &precision,
		}}
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

// nullLiteral returns a null literal of Spark's NULL type. It's the type of a
// column that is itself the Arrow null type, which has no value to type it by.
func nullLiteral() *connectpb.Expression_Literal {
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Null{
		Null: &connectpb.DataType{Kind: &connectpb.DataType_Null{Null: &connectpb.DataType_NULL{}}},
	}}
}

// typedNullLiteral returns a null of the Spark type that the Arrow type maps to,
// so every row of a column has the same type, including the null rows. A null
// of the Arrow null type stays untyped.
func typedNullLiteral(dt arrow.DataType) (*connectpb.Expression_Literal, error) {
	if dt.ID() == arrow.NULL {
		return nullLiteral(), nil
	}
	sparkType, err := sparkDataType(dt)
	if err != nil {
		return nil, err
	}
	return &connectpb.Expression_Literal{LiteralType: &connectpb.Expression_Literal_Null{Null: sparkType}}, nil
}

// sparkDataType maps an Arrow type to the Spark type of the literal columnToLiteral
// makes for its values.
func sparkDataType(dt arrow.DataType) (*connectpb.DataType, error) {
	switch t := dt.(type) {
	case *arrow.BooleanType:
		return &connectpb.DataType{Kind: &connectpb.DataType_Boolean_{Boolean: &connectpb.DataType_Boolean{}}}, nil
	case *arrow.Int8Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Byte_{Byte: &connectpb.DataType_Byte{}}}, nil
	case *arrow.Int16Type, *arrow.Uint8Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Short_{Short: &connectpb.DataType_Short{}}}, nil
	case *arrow.Int32Type, *arrow.Uint16Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Integer_{Integer: &connectpb.DataType_Integer{}}}, nil
	case *arrow.Int64Type, *arrow.Uint32Type, *arrow.Uint64Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Long_{Long: &connectpb.DataType_Long{}}}, nil
	case *arrow.Float16Type, *arrow.Float32Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Float_{Float: &connectpb.DataType_Float{}}}, nil
	case *arrow.Float64Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Double_{Double: &connectpb.DataType_Double{}}}, nil
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return &connectpb.DataType{Kind: &connectpb.DataType_String_{String_: &connectpb.DataType_String{}}}, nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.FixedSizeBinaryType, *arrow.BinaryViewType:
		return &connectpb.DataType{Kind: &connectpb.DataType_Binary_{Binary: &connectpb.DataType_Binary{}}}, nil
	case *arrow.Date32Type, *arrow.Date64Type:
		return &connectpb.DataType{Kind: &connectpb.DataType_Date_{Date: &connectpb.DataType_Date{}}}, nil
	case *arrow.TimestampType:
		if t.TimeZone != "" {
			return &connectpb.DataType{Kind: &connectpb.DataType_Timestamp_{Timestamp: &connectpb.DataType_Timestamp{}}}, nil
		}
		return &connectpb.DataType{Kind: &connectpb.DataType_TimestampNtz{TimestampNtz: &connectpb.DataType_TimestampNTZ{}}}, nil
	case *arrow.Decimal128Type:
		prec, scale := t.Precision, t.Scale
		return &connectpb.DataType{Kind: &connectpb.DataType_Decimal_{Decimal: &connectpb.DataType_Decimal{Precision: &prec, Scale: &scale}}}, nil
	case *arrow.Decimal256Type:
		prec, scale := t.Precision, t.Scale
		return &connectpb.DataType{Kind: &connectpb.DataType_Decimal_{Decimal: &connectpb.DataType_Decimal{Precision: &prec, Scale: &scale}}}, nil
	case *arrow.Time32Type:
		_, precision, err := timeToNanos(0, t.Unit)
		if err != nil {
			return nil, err
		}
		return &connectpb.DataType{Kind: &connectpb.DataType_Time_{Time: &connectpb.DataType_Time{Precision: &precision}}}, nil
	case *arrow.Time64Type:
		_, precision, err := timeToNanos(0, t.Unit)
		if err != nil {
			return nil, err
		}
		return &connectpb.DataType{Kind: &connectpb.DataType_Time_{Time: &connectpb.DataType_Time{Precision: &precision}}}, nil
	case *arrow.DictionaryType:
		return sparkDataType(t.ValueType)
	}
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[spark] cannot bind null parameter of Arrow type %s", dt),
		Code: adbc.StatusNotImplemented,
	}
}

// timeToNanos converts an Arrow time value to nanoseconds since midnight, and
// gives the precision Spark stores it with. Spark's TIME keeps microseconds at
// most, so nanoseconds are truncated to microseconds, as timestamps are.
func timeToNanos(v int64, unit arrow.TimeUnit) (int64, int32, error) {
	switch unit {
	case arrow.Second:
		return v * 1_000_000_000, 0, nil
	case arrow.Millisecond:
		return v * 1_000_000, 3, nil
	case arrow.Microsecond:
		return v * 1_000, 6, nil
	case arrow.Nanosecond:
		return v / 1_000 * 1_000, 6, nil
	default:
		return 0, 0, adbc.Error{
			Msg:  fmt.Sprintf("[spark] unsupported time unit %v", unit),
			Code: adbc.StatusNotImplemented,
		}
	}
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

// sameColumnTypes reports whether two result schemas have the same number of
// columns and the same type for each, with nested nullability counted. Two
// things are left out. Top-level nullability: Spark marks a column with no null
// as not nullable, so a row can differ from another only there. Names: Spark
// names an unaliased expression after its literal, so "1 + ?" is "(1 + 1)" for
// one value and "(1 + 2)" for another. Batches from later rows are re-labelled
// with the advertised schema.
func sameColumnTypes(a, b *arrow.Schema) bool {
	if len(a.Fields()) != len(b.Fields()) {
		return false
	}
	for i, f := range a.Fields() {
		if !arrow.TypeEqual(f.Type, b.Field(i).Type) {
			return false
		}
	}
	return true
}

// withNullableFields returns s with every top-level field nullable. Names, types,
// field metadata and schema metadata are kept.
func withNullableFields(s *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, len(s.Fields()))
	for i, f := range s.Fields() {
		f.Nullable = true
		fields[i] = f
	}
	md := s.Metadata()
	return arrow.NewSchema(fields, &md)
}

// formatColumns lists the columns of a result compactly, as "(v: decimal(5, 2))".
func formatColumns(s *arrow.Schema) string {
	parts := make([]string, 0, len(s.Fields()))
	for _, f := range s.Fields() {
		parts = append(parts, f.Name+": "+formatType(f.Type))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// formatType writes a type the way Spark writes it. Decimals are decimal(p, s).
func formatType(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.Decimal128Type:
		return fmt.Sprintf("decimal(%d, %d)", t.Precision, t.Scale)
	case *arrow.Decimal256Type:
		return fmt.Sprintf("decimal(%d, %d)", t.Precision, t.Scale)
	}
	return dt.String()
}

// newRowsReader executes the first row, to learn the schema, and returns a
// reader over all rows. rows must not be empty.
//
// The advertised schema is the first row's. When more than one row is bound,
// every top-level field is nullable, since a later row may hold a null where the
// first row held a value. A single row keeps its schema as it is.
func newRowsReader(open func([]*connectpb.Expression_Literal) (array.RecordReader, error), rows [][]*connectpb.Expression_Literal) (array.RecordReader, error) {
	first, err := open(rows[0])
	if err != nil {
		return nil, err
	}
	r := &rowsReader{open: open, rows: rows[1:], cur: first, refs: 1}
	if first != nil {
		r.rowSchema = first.Schema()
		r.schema = r.rowSchema
		if len(rows) > 1 {
			r.schema = withNullableFields(r.rowSchema)
		}
	}
	return r, nil
}

// rowsReader concatenates the results of one execution per bound row. Rows are
// executed as the reader is consumed, so their results stream like one query.
// Every batch it returns has the advertised schema.
type rowsReader struct {
	open func(args []*connectpb.Expression_Literal) (array.RecordReader, error)
	rows [][]*connectpb.Expression_Literal
	// schema is the advertised schema. rowSchema is row 0's own schema, which
	// later rows are compared with.
	schema    *arrow.Schema
	rowSchema *arrow.Schema
	// index is the row whose execution is current. Row 0 is the first.
	index int
	cur   array.RecordReader
	// wrapped is the current batch re-labelled with the advertised schema, if the
	// batch's own schema differs. It's released by the next Next or by Release.
	wrapped arrow.RecordBatch
	err     error
	refs    int64
}

func (r *rowsReader) Retain() {
	r.refs++
}

func (r *rowsReader) Release() {
	r.refs--
	if r.refs == 0 {
		r.dropWrapped()
		if r.cur != nil {
			r.cur.Release()
			r.cur = nil
		}
	}
}

func (r *rowsReader) dropWrapped() {
	if r.wrapped != nil {
		r.wrapped.Release()
		r.wrapped = nil
	}
}

func (r *rowsReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *rowsReader) Next() bool {
	r.dropWrapped()
	for {
		if r.cur != nil {
			if r.cur.Next() {
				return true
			}
			if err := r.cur.Err(); err != nil {
				r.err = err
				r.cur.Release()
				r.cur = nil
				return false
			}
			r.cur.Release()
			r.cur = nil
		}
		if len(r.rows) == 0 || r.err != nil {
			return false
		}
		args := r.rows[0]
		r.rows = r.rows[1:]
		r.index++
		rdr, err := r.open(args)
		if err != nil {
			r.err = err
			return false
		}
		// A later row must have the same column types. A query whose result
		// type depends on the bound values can't return one result for them.
		if r.rowSchema != nil && rdr != nil && !sameColumnTypes(rdr.Schema(), r.rowSchema) {
			r.err = adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg: fmt.Sprintf("[spark] parameter row %d gives result columns %s, but row 0 gave %s: "+
					"a query whose result type depends on the bound values can't return one result for several rows; "+
					"execute the rows separately",
					r.index, formatColumns(rdr.Schema()), formatColumns(r.rowSchema)),
			}
			rdr.Release()
			return false
		}
		r.cur = rdr
	}
}

// RecordBatch returns the current batch with the advertised schema. A batch
// whose schema differs, such as one with another nullability or column names,
// is re-labelled. The wrapper is released by the next Next or by Release.
func (r *rowsReader) RecordBatch() arrow.RecordBatch {
	if r.cur == nil {
		return nil
	}
	rec := r.cur.RecordBatch()
	if rec == nil || r.schema == nil || rec.Schema().Equal(r.schema) {
		return rec
	}
	if r.wrapped == nil {
		r.wrapped = array.NewRecordBatch(r.schema, rec.Columns(), rec.NumRows())
	}
	return r.wrapped
}

// Record is deprecated in arrow-go; it's RecordBatch.
func (r *rowsReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *rowsReader) Err() error {
	return r.err
}
