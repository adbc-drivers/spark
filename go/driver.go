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
	"maps"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type driverImpl struct {
	driverbase.DriverImplBase
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.DatabaseWithContext, error) {
	base, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}
	db := &databaseImpl{
		DatabaseImplBase: base,
	}
	opts = maps.Clone(opts)
	if err := db.SetOptions(ctx, opts); err != nil {
		return nil, err
	}
	return driverbase.NewDatabase(db), nil
}

// NewDriver creates a new driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	info := driverbase.DefaultDriverInfo("Apache Spark")
	err := info.RegisterInfoCode(adbc.InfoDriverName, "ADBC Driver for Apache Spark")
	if err != nil {
		panic(err)
	}
	// if infoVendorVersion != "" {
	// 	if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
	// 		panic(err)
	// 	}
	// }
	base := driverbase.NewDriverImplBase(info, alloc)
	base.ErrorHelper.DriverName = "spark"
	return &driverImpl{DriverImplBase: base}
}
