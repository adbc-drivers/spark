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

package spark_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	// "strings"
	"testing"

	driver "github.com/adbc-drivers/apache/go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type SparkQuirks struct {
	dsn string

	mem *memory.CheckedAllocator
}

func (s *SparkQuirks) SetupDriver(t *testing.T) adbc.Driver {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return driver.NewDriver(s.mem)
}

func (s *SparkQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	s.mem.AssertSize(t, 0)
}

func (s *SparkQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: s.dsn,
	}
}

func quoteIdentifier(ident string) string {
	// TODO:
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(ident, `"`, `""`))
}

func (q *SparkQuirks) CreateSampleTable(tableName string, r arrow.Record) (err error) {
	return errors.New("TBD")
}

func (q *SparkQuirks) DropTable(cnxn adbc.Connection, tblname string) (err error) {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	if err = stmt.SetSqlQuery(`DROP TABLE IF EXISTS ` + quoteIdentifier(tblname)); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *SparkQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	return arrow.Metadata{}
}

func (q *SparkQuirks) Alloc() memory.Allocator                     { return q.mem }
func (q *SparkQuirks) BindParameter(idx int) string                { return fmt.Sprintf("$%d", idx+1) }
func (q *SparkQuirks) SupportsBulkIngest(string) bool              { return true }
func (q *SparkQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *SparkQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *SparkQuirks) SupportsExecuteSchema() bool                 { return true }
func (q *SparkQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *SparkQuirks) SupportsPartitionedData() bool               { return false }
func (q *SparkQuirks) SupportsStatistics() bool                    { return false }
func (q *SparkQuirks) SupportsTransactions() bool                  { return true }
func (q *SparkQuirks) SupportsGetParameterSchema() bool            { return true }
func (q *SparkQuirks) SupportsDynamicParameterBinding() bool       { return true }
func (q *SparkQuirks) SupportsErrorIngestIncompatibleSchema() bool { return true }
func (q *SparkQuirks) Catalog() string                             { return "dev" }
func (q *SparkQuirks) DBSchema() string                            { return "public" }
func (q *SparkQuirks) GetMetadata(code adbc.InfoCode) any {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver for Apache Spark"
	// runtime/debug.ReadBuildInfo doesn't currently work for tests
	// github.com/golang/go/issues/33976
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "Apache Spark"
	}

	return nil
}

func withQuirks(t *testing.T, fn func(*SparkQuirks)) {
	uri := os.Getenv("SPARK_URI")
	if uri == "" {
		t.Skip("no SPARK_URI defined, skip driver tests")
	}

	q := &SparkQuirks{dsn: uri}
	fn(q)
}

func TestValidation(t *testing.T) {
	withQuirks(t, func(q *SparkQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

func TestDriver(t *testing.T) {
	withQuirks(t, func(q *SparkQuirks) {
		suite.Run(t, &DriverTests{Quirks: q})
	})
}

// -------------------- Additional Tests --------------------

type DriverTests struct {
	suite.Suite

	Quirks *SparkQuirks

	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (suite *DriverTests) SetupTest() {
	var err error
	suite.ctx = context.Background()
	suite.driver = suite.Quirks.SetupDriver(suite.T())
	suite.db, err = suite.driver.NewDatabase(suite.Quirks.DatabaseOptions())
	suite.NoError(err)
	suite.cnxn, err = suite.db.Open(suite.ctx)
	suite.NoError(err)
	suite.stmt, err = suite.cnxn.NewStatement()
	suite.NoError(err)
}

func (suite *DriverTests) TearDownTest() {
	suite.NoError(suite.stmt.Close())
	suite.NoError(suite.cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.driver)
	suite.cnxn = nil
	suite.NoError(suite.db.Close())
	suite.db = nil
	suite.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (suite *DriverTests) TestSelect() {
	for _, testCase := range []selectCase{
		{
			name:  "boolean",
			query: "SELECT TRUE AS istrue",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.FixedWidthTypes.Boolean,
					Nullable: true,
				},
			}, nil),
			expected: `[{"istrue": true}]`,
		},
		{
			name:  "int16",
			query: "SELECT CAST(42 AS SMALLINT) AS theanswer",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int16,
					Nullable: true,
				},
			}, nil),
			expected: `[{"theanswer": 42}]`,
		},
		{
			name:  "int32",
			query: "SELECT 42 AS theanswer",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
				},
			}, nil),
			expected: `[{"theanswer": 42}]`,
		},
		{
			name:  "int64",
			query: "SELECT CAST(42 AS BIGINT) AS theanswer",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
				},
			}, nil),
			expected: `[{"theanswer": 42}]`,
		},
		{
			name:  "float32",
			query: "SELECT 3.25F AS value",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
				},
			}, nil),
			expected: `[{"value": 3.25}]`,
		},
		{
			name:  "float64",
			query: "SELECT 3.25D AS value",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
				},
			}, nil),
			expected: `[{"value": 3.25}]`,
		},
		{
			name:  "decimal128_0",
			query: "SELECT CAST(0 AS NUMERIC(38, 10)) AS amount",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "amount",
					Type:     &arrow.Decimal128Type{Precision: 38, Scale: 10},
					Nullable: true,
				},
			}, nil),
			expected: `[{"amount": "0"}]`,
		},
		{
			name:  "decimal128_1",
			query: "SELECT CAST(123.45 AS NUMERIC(5, 2)) AS amount",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "amount",
					Type:     &arrow.Decimal128Type{Precision: 5, Scale: 2},
					Nullable: true,
				},
			}, nil),
			expected: `[{"amount": "123.45"}]`,
		},
		{
			name:  "decimal128_2",
			query: "SELECT CAST(123450000000.0000000001 AS NUMERIC(38, 10)) AS amount",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "amount",
					Type:     &arrow.Decimal128Type{Precision: 38, Scale: 10},
					Nullable: true,
				},
			}, nil),
			expected: `[{"amount": "123450000000.0000000001"}]`,
		},
		{
			name:  "string",
			query: "SELECT 'hello world' AS greeting",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
				},
			}, nil),
			expected: `[{"greeting": "hello world"}]`,
		},
		{
			name:  "blob",
			query: "SELECT X'e38193e38293e381abe381a1e381afe38081e4b896e7958cefbc81' AS greeting",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
				},
			}, nil),
			expected: `[{"greeting": "44GT44KT44Gr44Gh44Gv44CB5LiW55WM77yB"}]`,
		},
		{
			name:  "date",
			query: "SELECT DATE '2025-01-01' AS date",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "date",
					Type:     arrow.FixedWidthTypes.Date32,
					Nullable: true,
				},
			}, nil),
			expected: `[{"date": "2025-01-01"}]`,
		},
		{
			name:  "timestamp",
			query: "SELECT TIMESTAMP_NTZ '1971-01-02 01:02:03.456789' AS time",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name: "time",
					Type: &arrow.TimestampType{
						Unit:     arrow.Microsecond,
						TimeZone: "UTC",
					},
					Nullable: true,
				},
			}, nil),
			expected: `[{"time": "1971-01-02 01:02:03.456789"}]`,
		},
		{
			name:  "timestamptz",
			query: "SELECT TIMESTAMP '1971-01-02 01:02:03.456789' AS time",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name: "time",
					Type: &arrow.TimestampType{
						Unit:     arrow.Microsecond,
						TimeZone: "UTC",
					},
					Nullable: true,
				},
			}, nil),
			expected: `[{"time": "1971-01-02 01:02:03.456789"}]`,
		},
	} {
		suite.Run(testCase.name, func() {
			suite.NoError(suite.stmt.SetSqlQuery(testCase.query))

			rdr, rows, err := suite.stmt.ExecuteQuery(context.Background())
			suite.NoError(err)
			defer rdr.Release()

			suite.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			suite.Equal(int64(-1), rows)
			suite.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			suite.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.Record()
			suite.NotNil(rec)

			suite.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			suite.False(rdr.Next())
			suite.NoError(rdr.Err())

		})
	}
}

func (suite *DriverTests) TestSelectNoResult() {
	suite.NoError(suite.stmt.SetSqlQuery("DROP TABLE IF EXISTS no_result"))

	rdr, rows, err := suite.stmt.ExecuteQuery(context.Background())
	suite.NoError(err)
	defer rdr.Release()

	// TODO(lidavidm): move this to validation suite
	// XXX: Spark is weird and apparently returns a field
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "Result",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
	}, nil)
	suite.Truef(schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", schema, rdr.Schema())
	suite.Equal(int64(-1), rows)
	suite.False(rdr.Next())
	suite.NoError(rdr.Err())
}

type SparkTestSuite struct {
	suite.Suite
	uri    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (s *SparkTestSuite) SetupSuite() {
	var err error
	s.uri = os.Getenv("SPARK_URI")

	if s.uri == "" {
		s.T().Skip("no SPARK_URI defined, skip driver tests")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = driver.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: s.uri,
	})
	s.NoError(err)

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})
	logger := slog.New(handler)
	s.db.(adbc.DatabaseLogging).SetLogger(logger)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement()
	s.NoError(err)
}

func (s *SparkTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close())
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close())
	}
	if s.db != nil {
		s.NoError(s.db.Close())
	}
	s.mem.AssertSize(s.T(), 0)
}
