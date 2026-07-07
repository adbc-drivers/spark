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

package spark

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/driverbase/arrowext"
	"github.com/adbc-drivers/spark/go/internal/sparkbase"
	"github.com/adbc-drivers/spark/go/sparkutil"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

type statementImpl struct {
	driverbase.StatementImplBase
	cnxn *connectionImpl

	query  string
	params array.RecordReader
	ingest bulkIngestOptions
}

func (st *statementImpl) clearQueryState() {
	// used to reset some common parameters when the query is changed
	st.query = ""
}

func (st *statementImpl) clearIngestState() {
	// used to reset some common parameters when the query is changed
	st.ingest.Clear()
}

func (st *statementImpl) Close(ctx context.Context) error {
	if st.params != nil {
		st.params.Release()
		st.params = nil
	}
	if st.cnxn == nil {
		return adbc.Error{
			Msg:  "[spark] statement not initialized or already closed",
			Code: adbc.StatusInvalidState,
		}
	}
	st.cnxn = nil
	return nil
}

func (st *statementImpl) GetOption(ctx context.Context, key string) (string, error) {
	return "", adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statementImpl) GetOptionBytes(ctx context.Context, key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statementImpl) GetOptionInt(ctx context.Context, key string) (int64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statementImpl) GetOptionDouble(ctx context.Context, key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

// SetOption sets a string option on this statement
func (st *statementImpl) SetOption(ctx context.Context, key string, val string) error {
	if ok, err := st.ingest.SetOption(&st.ErrorHelper, key, val); err != nil {
		st.clearQueryState()
		return err
	} else if ok {
		return nil
	}

	switch key {
	case sparkutil.StatementOptionIngestStagingAreaURI:
		parsed, err := url.Parse(val)
		if err != nil {
			return sparkbase.ErrToAdbcErr(adbc.StatusInternal, err, "parse staging area URI `%s`", val)
		}
		st.clearQueryState()
		st.ingest.staging = parsed
	case sparkutil.OptionIngestLocation:
		st.clearQueryState()
		st.ingest.location = val
	case sparkutil.OptionIngestS3BaseEndpoint:
		st.ingest.s3BaseEndpoint = val
	case sparkutil.OptionIngestS3UsePathStyle:
		switch strings.ToLower(val) {
		case "true":
			st.ingest.s3UsePathStyle = true
		case "false":
			st.ingest.s3UsePathStyle = false
		default:
			return sparkbase.InvalidOptionErr(key, val)
		}
	default:
		return adbc.Error{
			Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
			Code: adbc.StatusNotImplemented,
		}
	}
	return nil
}

func (st *statementImpl) SetOptionBytes(ctx context.Context, key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statementImpl) SetOptionInt(ctx context.Context, key string, value int64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statementImpl) SetOptionDouble(ctx context.Context, key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statementImpl) SetSqlQuery(ctx context.Context, query string) error {
	st.clearIngestState()
	st.query = query
	return nil
}

func (st *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if st.ingest.IsSet() {
		n, err := st.executeIngest(ctx)
		return arrowext.EmptyReader{}, n, err
	} else if st.query == "" {
		return nil, -1, adbc.Error{
			Msg:  "[spark] no query set",
			Code: adbc.StatusInvalidState,
		}
	}
	return st.cnxn.client.ExecuteQuery(ctx, sparkbase.QueryContext{
		Mem:   st.cnxn.Alloc,
		Log:   st.cnxn.Logger,
		Query: st.query,
	})
}

func (st *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if st.ingest.IsSet() {
		return st.executeIngest(ctx)
	}
	return st.cnxn.client.ExecuteUpdate(ctx, sparkbase.QueryContext{
		Mem:   st.cnxn.Alloc,
		Log:   st.cnxn.Logger,
		Query: st.query,
	})
}

func (st *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, sparkbase.ErrTBD
}

func (st *statementImpl) Prepare(ctx context.Context) error {
	if st.query == "" {
		return adbc.Error{
			Msg:  "[spark] no query set",
			Code: adbc.StatusInvalidState,
		}
	}
	// no-op
	return nil
}

func (st *statementImpl) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	return st.ErrorHelper.NotImplemented("SetSubstraitPlan not supported")
}

func (st *statementImpl) Bind(_ context.Context, values arrow.RecordBatch) error {
	if st.params != nil {
		st.params.Release()
		st.params = nil
	}
	stream, err := array.NewRecordReader(values.Schema(), []arrow.RecordBatch{values})
	if err != nil {
		// Should never happen as error is for schema mismatch
		return adbc.Error{
			Msg:  "[spark] failed to create record reader",
			Code: adbc.StatusInternal,
		}
	}
	st.params = stream
	return nil
}

func (st *statementImpl) BindStream(_ context.Context, stream array.RecordReader) error {
	if st.params != nil {
		st.params.Release()
		st.params = nil
	}
	if stream != nil {
		st.params = stream
		st.params.Retain()
	}
	return nil
}

func (st *statementImpl) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, sparkbase.ErrTBD
}

func (st *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, st.ErrorHelper.NotImplemented("ExecutePartitions not supported")
}

func (st *statementImpl) executeIngest(ctx context.Context) (int64, error) {
	if st.ingest.staging == nil {
		return -1, st.ErrorHelper.InvalidState("must set %s to ingest data", sparkutil.StatementOptionIngestStagingAreaURI)
	} else if st.ingest.staging.Scheme != "s3" && st.ingest.staging.Scheme != "s3a" {
		return -1, st.ErrorHelper.NotImplemented("staging area scheme `%s` not supported", st.ingest.staging.Scheme)
	}

	bucket := st.ingest.staging.Hostname()
	prefix := st.ingest.staging.Path
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")

	sdkConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return -1, sparkbase.ErrToAdbcErr(adbc.StatusInternal, err, "load AWS SDK config")
	}

	logger := st.cnxn.Logger.With("op", "bulkingest")
	s3Client := s3.NewFromConfig(sdkConfig, func(opts *s3.Options) {
		if st.ingest.s3BaseEndpoint != "" {
			opts.BaseEndpoint = new(st.ingest.s3BaseEndpoint)
		}
		opts.UsePathStyle = st.ingest.s3UsePathStyle
	})
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) { //nolint:staticcheck
		u.PartSize = 5 * 1024 * 1024
	})

	// N.B. for now this all assumes S3, but in theory we should be able
	// to support things like POSIX filesystems (e.g. for a Spark
	// deployment with a network filesystem), GCS, Azure, HDFS, etc.
	impl := &bulkIngestImpl{
		logger:   logger,
		mem:      st.cnxn.Alloc,
		client:   st.cnxn.client,
		s3Client: s3Client,
		uploader: uploader,
		options:  st.ingest,
		bucket:   bucket,
		prefix:   prefix,
		keyUUID:  uuid.Must(uuid.NewV7()),
	}
	manager := &driverbase.BulkIngestManager{
		Impl:        impl,
		ErrorHelper: &st.ErrorHelper,
		Logger:      logger,
		Alloc:       st.cnxn.Alloc,
		Ctx:         ctx,
		Options:     st.ingest.BulkIngestOptions,
		Data:        st.params,
	}
	st.params = nil
	defer manager.Close()

	if err := manager.Init(); err != nil {
		return -1, err
	}
	return manager.ExecuteIngest()
}
