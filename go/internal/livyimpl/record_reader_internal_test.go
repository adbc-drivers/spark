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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fabric serializes wide numeric and boolean values as JSON strings; the
// value coercion helpers must decode them without precision loss.

func TestToInt64StringEncoded(t *testing.T) {
	// 2^53 + 1: not representable as float64, so string decoding must not
	// round-trip through a float.
	v, ok := toInt64("9007199254740993")
	require.True(t, ok)
	assert.Equal(t, int64(9007199254740993), v)
	_, ok = toInt64("not a number")
	assert.False(t, ok)
}

func TestToFloat64StringEncoded(t *testing.T) {
	v, ok := toFloat64("1.5")
	require.True(t, ok)
	assert.Equal(t, 1.5, v)
	_, ok = toFloat64("not a number")
	assert.False(t, ok)
}

func TestAppendValueStringEncoded(t *testing.T) {
	alloc := memory.NewGoAllocator()

	t.Run("int64 from string", func(t *testing.T) {
		b := array.NewInt64Builder(alloc)
		defer b.Release()
		err := appendValueToBuilder(b, "9007199254740993", arrow.PrimitiveTypes.Int64)
		require.NoError(t, err)
		arr := b.NewInt64Array()
		defer arr.Release()
		assert.Equal(t, int64(9007199254740993), arr.Value(0))
	})

	t.Run("bool from string", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		err := appendValueToBuilder(b, "true", arrow.FixedWidthTypes.Boolean)
		require.NoError(t, err)
		err = appendValueToBuilder(b, false, arrow.FixedWidthTypes.Boolean)
		require.NoError(t, err)
		arr := b.NewBooleanArray()
		defer arr.Release()
		assert.True(t, arr.Value(0))
		assert.False(t, arr.Value(1))
	})

	t.Run("invalid bool string errors", func(t *testing.T) {
		b := array.NewBooleanBuilder(alloc)
		defer b.Release()
		err := appendValueToBuilder(b, "not a bool", arrow.FixedWidthTypes.Boolean)
		require.Error(t, err)
	})

	t.Run("float64 from string", func(t *testing.T) {
		b := array.NewFloat64Builder(alloc)
		defer b.Release()
		err := appendValueToBuilder(b, "1.5", arrow.PrimitiveTypes.Float64)
		require.NoError(t, err)
		arr := b.NewFloat64Array()
		defer arr.Release()
		assert.Equal(t, 1.5, arr.Value(0))
	})
}
