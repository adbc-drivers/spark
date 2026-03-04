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

	"github.com/adbc-drivers/apache/go/internal/sparkbase"
	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	clientFactory sparkbase.SparkClientFactory
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	return d.DatabaseImplBase.GetOption(key)
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	var err error
	d.clientFactory, err = newSparkClientFactory(options)
	if err != nil {
		return err
	}

	for key := range options {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[spark] Unknown option %s", key),
		}
	}

	return nil
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
	}

	client, err := d.clientFactory(ctx)
	if err != nil {
		return nil, err
	}

	if err := conn.Init(client); err != nil {
		return nil, err
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDriverInfoPreparer(conn).
		WithDbObjectsEnumerator(client).
		Connection(), nil
}

func (d *databaseImpl) Close() error {
	return nil
}
