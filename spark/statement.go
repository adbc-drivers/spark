// Copyright (c) 2025 Columnar Technologies, Inc.
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
	"fmt"

	"github.com/adbc-drivers/apache/spark/internal/hiveserver2"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type statementImpl struct {
	driverbase.StatementImplBase
	cnxn *connectionImpl

	query string
}

// func (st *statementImpl) clearQueryState() {
// 	// used to reset some common parameters when the query is changed
// 	st.query = ""
// }

func (st *statementImpl) clearIngestState() {
	// used to reset some common parameters when the query is changed
}

func (st *statementImpl) Close() error {
	return nil
}

func (st *statementImpl) GetOption(key string) (string, error) {
	return "", adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statementImpl) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statementImpl) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statementImpl) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

// SetOption sets a string option on this statement
func (st *statementImpl) SetOption(key string, val string) error {
	switch key {
	default:
		return adbc.Error{
			Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
			Code: adbc.StatusNotImplemented,
		}
	}
	// return nil
}

func (st *statementImpl) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statementImpl) SetOptionInt(key string, value int64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statementImpl) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statementImpl) SetSqlQuery(query string) error {
	st.clearIngestState()
	st.query = query
	return nil
}

func (st *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: st.cnxn.session,
		Statement:     st.query,
	}
	rdr, err := newRecordReader(context.Background(), st.cnxn.Alloc, st.cnxn.client, req)
	if err != nil {
		return nil, -1, err
	}
	return rdr, -1, nil
}

func (st *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	req := &hiveserver2.TExecuteStatementReq{
		SessionHandle: st.cnxn.session,
		Statement:     st.query,
	}
	resp, err := driverbase.WithShared(st.cnxn.client, func(client *hiveserver2.TCLIServiceClient) (*hiveserver2.TExecuteStatementResp, error) {
		return client.ExecuteStatement(ctx, req)
	})
	if err != nil {
		return -1, errToAdbcErr(adbc.StatusIO, err, "execute statement")
	} else if err = statusToAdbcErr(resp.Status, "execute statement"); err != nil {
		return -1, err
	}
	// TODO: if HasResultSet, do we have to explicitly free it?
	if resp.OperationHandle.ModifiedRowCount == nil {
		return -1, nil
	}
	return int64(*resp.OperationHandle.ModifiedRowCount), nil
}

func (st *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, errTBD
}

func (st *statementImpl) Prepare(ctx context.Context) error {
	return errTBD
}

func (st *statementImpl) SetSubstraitPlan(plan []byte) error {
	return errTBD
}

func (st *statementImpl) Bind(_ context.Context, values arrow.Record) error {
	return errTBD
}

func (st *statementImpl) BindStream(_ context.Context, stream array.RecordReader) error {
	return errTBD
}

func (st *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, errTBD
}

func (st *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, errTBD
}
