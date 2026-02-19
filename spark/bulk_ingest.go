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
	"log/slog"
	"net/url"
	"strings"

	"github.com/adbc-drivers/apache/spark/internal/sparkbase"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

type bulkIngestOptions struct {
	driverbase.BulkIngestOptions

	staging *url.URL
}

func NewBulkIngestOptions() bulkIngestOptions {
	return bulkIngestOptions{
		BulkIngestOptions: driverbase.NewBulkIngestOptions(),
	}
}

type bulkIngestPendingCopy struct {
	bucket string
	key    string
	rows   int64
}

func (pendingCopy *bulkIngestPendingCopy) Rows() int64 {
	return pendingCopy.rows
}

func (pendingCopy *bulkIngestPendingCopy) String() string {
	return fmt.Sprintf("s3a://%s/%s", pendingCopy.bucket, pendingCopy.key)
}

type bulkIngestImpl struct {
	logger   *slog.Logger
	mem      memory.Allocator
	client   sparkbase.SparkClient
	s3Client *s3.Client
	uploader *manager.Uploader
	options  bulkIngestOptions
	bucket   string
	prefix   string
	// All uploaded files will have this UUID in them
	keyUUID uuid.UUID
}

func (bi *bulkIngestImpl) generateObjectKey() string {
	if bi.prefix == "" {
		return fmt.Sprintf("%s/%s/%s.parquet", bi.options.TableName, bi.keyUUID, uuid.Must(uuid.NewV7()))
	} else {
		return fmt.Sprintf("%s/%s/%s/%s.parquet", bi.prefix, bi.options.TableName, bi.keyUUID, uuid.Must(uuid.NewV7()))
	}
}

func (bi *bulkIngestImpl) Copy(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	// TODO(lidavidm): catalog, schema
	query.WriteString(sparkbase.QuoteIdentifier(bi.options.TableName))
	query.WriteString(" SELECT * FROM parquet.`")
	query.WriteString(chunk.(*bulkIngestPendingCopy).String())
	query.WriteString("`")

	_, err := bi.client.ExecuteUpdate(ctx, sparkbase.QueryContext{
		Mem:   bi.mem,
		Query: query.String(),
	})
	if err != nil {
		return sparkbase.ErrToAdbcErr(adbc.StatusInternal, err, "INSERT")
	}
	return nil
}

func (bi *bulkIngestImpl) CreateSink(ctx context.Context, options *driverbase.BulkIngestOptions) (driverbase.BulkIngestSink, error) {
	return &driverbase.BufferBulkIngestSink{}, nil
}

func (bi *bulkIngestImpl) CreateTable(ctx context.Context, schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) error {
	if stmts, err := bi.createTableStatement(schema, ifTableExists, ifTableMissing); err != nil {
		return err
	} else if stmts != nil {
		bi.logger.Debug("creating table", "table", bi.options.TableName, "stmt", stmts)
		for _, stmt := range stmts {
			_, err := bi.client.ExecuteUpdate(ctx, sparkbase.QueryContext{
				Mem:   bi.mem,
				Query: stmt,
			})
			if err != nil {
				return sparkbase.ErrToAdbcErr(adbc.StatusInternal, err, "CREATE TABLE")
			}
		}
		bi.logger.Debug("created table", "table", bi.options.TableName)
	}
	return nil
}

func (bi *bulkIngestImpl) Upload(ctx context.Context, chunk driverbase.BulkIngestPendingUpload) (driverbase.BulkIngestPendingCopy, error) {
	parquetMimeType := "application/vnd.apache.parquet"
	ifNoneMatch := "*"

	key := bi.generateObjectKey()
	_, err := bi.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      &bi.bucket,
		Key:         &key,
		Body:        &chunk.Data.(*driverbase.BufferBulkIngestSink).Buffer,
		ContentType: &parquetMimeType,
		IfNoneMatch: &ifNoneMatch,
	})
	bi.logger.Debug("uploaded data to S3", "bucket", bi.bucket, "key", key, "err", err)
	if err != nil {
		// TODO(lidavidm): separate error handler for S3
		return nil, sparkbase.ErrToAdbcErr(adbc.StatusInternal, err, "upload data to S3")
	}

	return &bulkIngestPendingCopy{
		bucket: bi.bucket,
		key:    key,
		rows:   chunk.Rows,
	}, nil
}

func (bi *bulkIngestImpl) Delete(ctx context.Context, chunk driverbase.BulkIngestPendingCopy) error {
	pendingFile := chunk.(*bulkIngestPendingCopy)
	_, err := bi.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &pendingFile.bucket,
		Key:    &pendingFile.key,
	})
	if err != nil {
		return sparkbase.ErrToAdbcErr(adbc.StatusInternal, err, "delete temporary S3 object")
	}
	return nil
}

func (bi *bulkIngestImpl) createTableStatement(schema *arrow.Schema, ifTableExists driverbase.BulkIngestTableExistsBehavior, ifTableMissing driverbase.BulkIngestTableMissingBehavior) ([]string, error) {
	var stmts []string
	var b strings.Builder

	switch ifTableExists {
	case driverbase.BulkIngestTableExistsError:
		// Do nothing
	case driverbase.BulkIngestTableExistsIgnore:
		// Do nothing
	case driverbase.BulkIngestTableExistsDrop:
		b.WriteString("DROP TABLE IF EXISTS ")
		// if bi.options.CatalogName != "" {
		// 	b.WriteString(quoteIdentifier(bi.options.CatalogName))
		// 	b.WriteString(".")
		// }
		// b.WriteString(quoteIdentifier(bi.schema))
		// b.WriteString(".")
		b.WriteString(sparkbase.QuoteIdentifier(bi.options.TableName))
		stmts = append(stmts, b.String())
		b.Reset()
	}

	switch ifTableMissing {
	case driverbase.BulkIngestTableMissingError:
		// Do nothing
	case driverbase.BulkIngestTableMissingCreate:
		if bi.options.Temporary {
			// TODO:
		} else {
			b.WriteString("CREATE TABLE ")
		}
		if ifTableExists == driverbase.BulkIngestTableExistsIgnore {
			b.WriteString("IF NOT EXISTS ")
		}

		// TODO:
		// if bi.options.CatalogName != "" {
		// 	b.WriteString(quoteIdentifier(bi.options.CatalogName))
		// 	b.WriteString(".")
		// }
		// if bi.options.SchemaName != "" {
		// 	b.WriteString(quoteIdentifier(bi.options.SchemaName))
		// 	b.WriteString(".")
		// }

		b.WriteString(sparkbase.QuoteIdentifier(bi.options.TableName))
		b.WriteString(" (")

		for i, field := range schema.Fields() {
			if i > 0 {
				b.WriteString(", ")
			}

			b.WriteString(sparkbase.QuoteIdentifier(field.Name))

			switch field.Type.ID() {
			case arrow.BINARY:
				b.WriteString(" BINARY")
			case arrow.BOOL:
				b.WriteString(" BOOLEAN")
			case arrow.DATE32:
				b.WriteString(" DATE")
			case arrow.DECIMAL128, arrow.DECIMAL256:
				dec := field.Type.(arrow.DecimalType)
				b.WriteString(fmt.Sprintf("DECIMAL(%d, %d)", dec.GetPrecision(), dec.GetScale()))
			case arrow.FLOAT32:
				b.WriteString(" REAL")
			case arrow.FLOAT64:
				b.WriteString(" DOUBLE")
			case arrow.INT16:
				b.WriteString(" SMALLINT")
			case arrow.INT32:
				b.WriteString(" INTEGER")
			case arrow.INT64:
				b.WriteString(" BIGINT")
			case arrow.STRING:
				b.WriteString(" STRING")
			case arrow.TIMESTAMP:
				ts := field.Type.(*arrow.TimestampType)
				if ts.TimeZone != "" {
					b.WriteString(" TIMESTAMP")
				} else {
					b.WriteString(" TIMESTAMP_NTZ")
				}
			default:
				return nil, adbc.Error{
					Msg:  fmt.Sprintf("[spark] Unsupported type %s", field.Type),
					Code: adbc.StatusNotImplemented,
				}
			}

			if !field.Nullable {
				b.WriteString(" NOT NULL")
			}
		}

		b.WriteString(")")
		stmts = append(stmts, b.String())
	}
	return stmts, nil
}
