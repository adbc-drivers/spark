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
	"net/url"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/driverbase/arrowext"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
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

func (st *statementImpl) Close() error {
	if st.params != nil {
		st.params.Release()
		st.params = nil
	}
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
	if ok, err := st.ingest.SetOption(&st.ErrorHelper, key, val); err != nil {
		st.clearQueryState()
		return err
	} else if ok {
		return nil
	}

	switch key {
	case StatementOptionIngestStagingAreaURI:
		parsed, err := url.Parse(val)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err, "parse staging area URI `%s`", val)
		}
		st.clearQueryState()
		st.ingest.staging = parsed
	default:
		return adbc.Error{
			Msg:  fmt.Sprintf("[spark] Unknown statement option '%s'", key),
			Code: adbc.StatusNotImplemented,
		}
	}
	return nil
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
	if st.ingest.IsSet() {
		n, err := st.executeIngest(ctx)
		return arrowext.EmptyReader{}, n, err
	}
	return st.cnxn.client.executeQuery(ctx, queryContext{
		mem:   st.cnxn.Alloc,
		query: st.query,
	})
}

func (st *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if st.ingest.IsSet() {
		return st.executeIngest(ctx)
	}
	return st.cnxn.client.executeUpdate(ctx, queryContext{
		mem:   st.cnxn.Alloc,
		query: st.query,
	})
}

func (st *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, errTBD
}

func (st *statementImpl) Prepare(ctx context.Context) error {
	return errTBD
}

func (st *statementImpl) SetSubstraitPlan(plan []byte) error {
	return st.ErrorHelper.NotImplemented("SetSubstraitPlan not supported")
}

func (st *statementImpl) Bind(_ context.Context, values arrow.Record) error {
	if st.params != nil {
		st.params.Release()
		st.params = nil
	}
	stream, err := array.NewRecordReader(values.Schema(), []arrow.Record{values})
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
	st.params = stream
	return nil
}

func (st *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, errTBD
}

func (st *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, st.ErrorHelper.NotImplemented("ExecutePartitions not supported")
}

func (st *statementImpl) executeIngest(ctx context.Context) (int64, error) {
	if st.ingest.staging == nil {
		return -1, st.ErrorHelper.InvalidState("must set %s to ingest data", StatementOptionIngestStagingAreaURI)
	} else if st.ingest.staging.Scheme != "s3" && st.ingest.staging.Scheme != "s3a" {
		return -1, st.ErrorHelper.NotImplemented("staging area scheme `%s` not supported", st.ingest.staging.Scheme)
	}

	bucket := st.ingest.staging.Hostname()
	prefix := st.ingest.staging.Path
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, "/")

	provider := awscredentials.NewStaticCredentialsProvider("admin", "password", "")
	sdkConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithCredentialsProvider(provider))
	if err != nil {
		return -1, errToAdbcErr(adbc.StatusInternal, err, "load AWS SDK config")
	}

	logger := st.cnxn.Logger.With("op", "bulkingest")
	s3Client := s3.NewFromConfig(sdkConfig, func(opts *s3.Options) {
		opts.BaseEndpoint = aws.String("http://localhost:9000/")
		opts.UsePathStyle = true
	})
	uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) {
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
