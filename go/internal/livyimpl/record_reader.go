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

package livyimpl

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// jsonRecordReader reads parsed row maps and converts them to Arrow records
type jsonRecordReader struct {
	alloc   memory.Allocator
	schema  *arrow.Schema
	rows    []map[string]any
	current int
	record  arrow.RecordBatch
	err     error
}

// newJSONRecordReader creates a new JSON record reader
func newJSONRecordReader(alloc memory.Allocator, schema *arrow.Schema, rows []map[string]any) (*jsonRecordReader, error) {
	return &jsonRecordReader{
		alloc:   alloc,
		schema:  schema,
		rows:    rows,
		current: -1,
	}, nil
}

// Schema returns the schema
func (r *jsonRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Next advances to the next record
func (r *jsonRecordReader) Next() bool {
	if r.current >= 0 && r.record != nil {
		r.record.Release()
		r.record = nil
	}

	if r.err != nil {
		return false
	}

	// Build all records at once (simplified approach)
	if r.current == -1 {
		r.current = 0
		if len(r.rows) == 0 {
			return false
		}

		// Convert all JSON rows to a single Arrow record
		record, err := r.buildRecord()
		if err != nil {
			r.err = err
			return false
		}

		r.record = record
		return true
	}

	// We return all data in one batch
	return false
}

// Record returns the current record
func (r *jsonRecordReader) Record() arrow.RecordBatch {
	return r.record
}

// RecordBatch returns the current record batch
func (r *jsonRecordReader) RecordBatch() arrow.RecordBatch {
	return r.record
}

// Err returns any error that occurred
func (r *jsonRecordReader) Err() error {
	return r.err
}

// Release releases resources
func (r *jsonRecordReader) Release() {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
}

// Retain increases the reference count
func (r *jsonRecordReader) Retain() {
	if r.record != nil {
		r.record.Retain()
	}
}

// buildRecord builds an Arrow record from pre-parsed row maps
func (r *jsonRecordReader) buildRecord() (arrow.RecordBatch, error) {
	if len(r.rows) == 0 {
		return array.NewRecordBatch(r.schema, []arrow.Array{}, 0), nil
	}

	bldr := array.NewRecordBuilder(r.alloc, r.schema)
	defer bldr.Release()

	for rowIdx, row := range r.rows {
		for colIdx, field := range r.schema.Fields() {
			value := row[field.Name]
			if err := appendValueToBuilder(bldr.Field(colIdx), value, field.Type); err != nil {
				return nil, fmt.Errorf("failed to append value for field %s (row %d): %w", field.Name, rowIdx, err)
			}
		}
	}

	return bldr.NewRecordBatch(), nil
}

// appendValueToBuilder appends a value to an Arrow array builder
func appendValueToBuilder(builder array.Builder, value any, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		if str, ok := value.(string); ok {
			b.Append(str)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}

	case *array.BinaryBuilder:
		if str, ok := value.(string); ok {
			decoded, err := hex.DecodeString(str)
			if err != nil {
				b.Append([]byte(str))
			} else {
				b.Append(decoded)
			}
		} else if bytes, ok := value.([]byte); ok {
			b.Append(bytes)
		} else if arr, ok := value.([]any); ok {
			bytes := make([]byte, len(arr))
			for i, v := range arr {
				if numVal, ok := toInt64(v); ok {
					bytes[i] = byte(numVal)
				} else {
					return fmt.Errorf("cannot convert array element %T to byte", v)
				}
			}
			b.Append(bytes)
		} else {
			return fmt.Errorf("cannot convert %T to binary", value)
		}

	case *array.BooleanBuilder:
		if boolVal, ok := value.(bool); ok {
			b.Append(boolVal)
		} else if str, ok := value.(string); ok {
			// Booleans may arrive as JSON strings (see toInt64).
			boolVal, err := strconv.ParseBool(str)
			if err != nil {
				return fmt.Errorf("cannot convert %q to bool", str)
			}
			b.Append(boolVal)
		} else {
			return fmt.Errorf("cannot convert %T to bool", value)
		}

	case *array.Int8Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(int8(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to int8", value)
		}

	case *array.Int16Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(int16(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to int16", value)
		}

	case *array.Int32Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(int32(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to int32", value)
		}

	case *array.Int64Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(numVal)
		} else {
			return fmt.Errorf("cannot convert %T to int64", value)
		}

	case *array.Uint8Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(uint8(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to uint8", value)
		}

	case *array.Uint16Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(uint16(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to uint16", value)
		}

	case *array.Uint32Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(uint32(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to uint32", value)
		}

	case *array.Uint64Builder:
		if numVal, ok := toInt64(value); ok {
			b.Append(uint64(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to uint64", value)
		}

	case *array.Float32Builder:
		if numVal, ok := toFloat64(value); ok {
			b.Append(float32(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to float32", value)
		}

	case *array.Float64Builder:
		if numVal, ok := toFloat64(value); ok {
			b.Append(numVal)
		} else {
			return fmt.Errorf("cannot convert %T to float64", value)
		}

	case *array.Decimal128Builder:
		str := fmt.Sprintf("%v", value)
		num, err := decimal128.FromString(str, b.Type().(*arrow.Decimal128Type).Precision, b.Type().(*arrow.Decimal128Type).Scale)
		if err != nil {
			return fmt.Errorf("cannot parse decimal128 value %q: %w", str, err)
		}
		b.Append(num)

	case *array.Date32Builder:
		// Parse date string or epoch days
		if str, ok := value.(string); ok {
			// Try to parse as date
			t, err := time.Parse("2006-01-02", str)
			if err != nil {
				return fmt.Errorf("cannot parse date: %w", err)
			}
			days := arrow.Date32FromTime(t)
			b.Append(days)
		} else if numVal, ok := toInt64(value); ok {
			b.Append(arrow.Date32(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to date32", value)
		}

	case *array.TimestampBuilder:
		// Parse timestamp string or epoch microseconds
		if str, ok := value.(string); ok {
			// Try to parse as timestamp
			t, err := time.Parse(time.RFC3339, str)
			if err != nil {
				// Try alternative format
				t, err = time.Parse("2006-01-02 15:04:05", str)
				if err != nil {
					return fmt.Errorf("cannot parse timestamp: %w", err)
				}
			}
			b.Append(arrow.Timestamp(t.UnixMicro()))
		} else if numVal, ok := toInt64(value); ok {
			b.Append(arrow.Timestamp(numVal))
		} else {
			return fmt.Errorf("cannot convert %T to timestamp", value)
		}

	case *array.ListBuilder:
		// Handle arrays
		if arr, ok := value.([]any); ok {
			b.Append(true)
			valueBuilder := b.ValueBuilder()
			listType := dataType.(*arrow.ListType)
			for _, elem := range arr {
				if err := appendValueToBuilder(valueBuilder, elem, listType.Elem()); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("cannot convert %T to list", value)
		}

	case *array.MapBuilder:
		// Handle maps
		if m, ok := value.(map[string]any); ok {
			b.Append(true)
			mapType := dataType.(*arrow.MapType)
			keyBuilder := b.KeyBuilder()
			itemBuilder := b.ItemBuilder()
			for k, v := range m {
				if err := appendValueToBuilder(keyBuilder, k, mapType.KeyType()); err != nil {
					return err
				}
				if err := appendValueToBuilder(itemBuilder, v, mapType.ItemType()); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("cannot convert %T to map", value)
		}

	case *array.StructBuilder:
		// Handle structs
		if m, ok := value.(map[string]any); ok {
			b.Append(true)
			structType := dataType.(*arrow.StructType)
			for i := range structType.NumFields() {
				field := structType.Field(i)
				fieldValue := m[field.Name]
				fieldBuilder := b.FieldBuilder(i)
				if err := appendValueToBuilder(fieldBuilder, fieldValue, field.Type); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("cannot convert %T to struct", value)
		}

	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// toInt64 converts various numeric types to int64
func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case string:
		// Some Livy implementations (e.g. Microsoft Fabric) serialize wide
		// numeric values as JSON strings to avoid precision loss.
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case json.Number:
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

// toFloat64 converts various numeric types to float64
func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case string:
		// See toInt64: numeric values may arrive as JSON strings.
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case json.Number:
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}
