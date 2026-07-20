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
	"context"
	"testing"

	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Fabric serializes wide numeric and boolean values as JSON strings; the
// value coercion helpers must decode them without precision loss.

func TestToInt64StringEncoded(t *testing.T) {
	// 2^53 + 1: not representable as float64, so string decoding must not
	// round-trip through a float.
	v, ok := toInt64("9007199254740993")
	if !ok || v != 9007199254740993 {
		t.Fatalf("toInt64(string) = (%d, %v)", v, ok)
	}
	if _, ok := toInt64("not a number"); ok {
		t.Fatal("toInt64 should reject non-numeric strings")
	}
}

func TestToFloat64StringEncoded(t *testing.T) {
	v, ok := toFloat64("1.5")
	if !ok || v != 1.5 {
		t.Fatalf("toFloat64(string) = (%v, %v)", v, ok)
	}
	if _, ok := toFloat64("not a number"); ok {
		t.Fatal("toFloat64 should reject non-numeric strings")
	}
}

func TestAppendValueStringEncoded(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("int64 from string", func(t *testing.T) {
		b := array.NewInt64Builder(alloc)
		defer b.Release()
		if err := appendValueToBuilder(b, "9007199254740993", arrow.PrimitiveTypes.Int64); err != nil {
			t.Fatalf("append: %v", err)
		}
		arr := b.NewInt64Array()
		defer arr.Release()
		if arr.Value(0) != 9007199254740993 {
			t.Fatalf("value = %d", arr.Value(0))
		}
	})

	t.Run("bool from string", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		if err := appendValueToBuilder(b, "true", arrow.FixedWidthTypes.Boolean); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := appendValueToBuilder(b, false, arrow.FixedWidthTypes.Boolean); err != nil {
			t.Fatalf("append native bool: %v", err)
		}
		arr := b.NewBooleanArray()
		defer arr.Release()
		if !arr.Value(0) || arr.Value(1) {
			t.Fatalf("values = %v, %v", arr.Value(0), arr.Value(1))
		}
	})

	t.Run("invalid bool string errors", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		if err := appendValueToBuilder(b, "not a bool", arrow.FixedWidthTypes.Boolean); err == nil {
			t.Fatal("expected error for non-boolean string")
		}
	})

	t.Run("float64 from string", func(t *testing.T) {
		b := array.NewFloat64Builder(alloc)
		defer b.Release()
		if err := appendValueToBuilder(b, "1.5", arrow.PrimitiveTypes.Float64); err != nil {
			t.Fatalf("append: %v", err)
		}
		arr := b.NewFloat64Array()
		defer arr.Release()
		if arr.Value(0) != 1.5 {
			t.Fatalf("value = %v", arr.Value(0))
		}
	})
}

// Session IDs are opaque strings; GetOption exposes them, GetOptionInt
// does not (they are not integers on all backends).
func TestSessionIDOptionGetters(t *testing.T) {
	ctx := context.Background()

	c := &livyClient{sessionID: SessionID(fabricGUID)}
	s, ok, err := c.GetOption(ctx, sparkutil.OptionLivySessionId)
	if err != nil || !ok || s != fabricGUID {
		t.Fatalf("GetOption = (%q, %v, %v)", s, ok, err)
	}
	if _, ok, _ := c.GetOptionInt(ctx, sparkutil.OptionLivySessionId); ok {
		t.Fatal("GetOptionInt should not report session IDs")
	}
}
