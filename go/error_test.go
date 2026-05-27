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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/suite"
)

func TestErrorMapping(t *testing.T) {
	suite.Run(t, &ErrorTestSuite{})
}

type ErrorTestSuite struct {
	SparkTestSuite
}

func (s *ErrorTestSuite) TestBadQuery() {
	s.NoError(s.stmt.SetSqlQuery(s.ctx, "this syntax ain't right"))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	var adbcError adbc.Error
	s.ErrorAs(err, &adbcError)

	s.Equal(adbc.StatusInvalidArgument, adbcError.Code)
	s.Equal([]byte("42601"), adbcError.SqlState[:])
}

func (s *ErrorTestSuite) TestNonexistentTable() {
	s.NoError(s.stmt.SetSqlQuery(s.ctx, "SELECT * FROM thistabledoesnotexist"))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	var adbcError adbc.Error
	s.ErrorAs(err, &adbcError)

	s.Equal(adbc.StatusNotFound, adbcError.Code)
	s.Equal([]byte("42P01"), adbcError.SqlState[:])
}
