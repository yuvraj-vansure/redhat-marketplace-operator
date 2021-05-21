// Copyright 2021 IBM Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/database"
	dqlite "github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/dqlite/driver"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/models"
	"gorm.io/gorm"
)

type DatabaseConfig struct {
	Name     string
	Dir      string
	Url      string
	Join     *[]string
	Verbose  bool
	Log      logr.Logger
	dqliteDB *sql.DB
	app      *app.App
	gormDB   *gorm.DB
}

// InitDB initializes the GORM connection and returns a connected struct
func (dc *DatabaseConfig) InitDB() (*database.Database, error) {
	database := &database.Database{}
	err := dc.initDqlite()
	if err != nil {
		return nil, err
	}

	dqliteDialector := dqlite.Open(dc.dqliteDB)
	database.DB, err = gorm.Open(dqliteDialector, &gorm.Config{})
	if err != nil {
		return nil, err
	}

	database.Log = dc.Log
	dc.gormDB = database.DB
	return database, err
}

// initDqlite initializes the underlying dqlite database
func (dc *DatabaseConfig) initDqlite() error {
	dc.Dir = filepath.Join(dc.Dir, dc.Url)
	if err := os.MkdirAll(dc.Dir, 0755); err != nil {
		return errors.Wrapf(err, "can't create %s", dc.Dir)
	}
	logFunc := func(l client.LogLevel, format string, a ...interface{}) {
		if !dc.Verbose {
			return
		}
		s := fmt.Sprintf(fmt.Sprintf("%s", format), a...)
		dc.Log.Info(strings.TrimRight(s, "\n"))
	}

	app, err := app.New(dc.Dir, app.WithAddress(dc.Url), app.WithCluster(*dc.Join), app.WithLogFunc(logFunc))
	if err != nil {
		return err
	}

	if err := app.Ready(context.Background()); err != nil {
		return err
	}

	conn, err := app.Open(context.Background(), dc.Name)
	if err != nil {
		return err
	}

	dc.app = app
	dc.dqliteDB = conn
	return conn.Ping()
}

// TryMigrate ensures that only the leader performs database migration
func (dc *DatabaseConfig) TryMigrate(ctx context.Context) error {
	dc.Log.Info("Verifying leadership")
	isLeader, err := IsLeader(dc.app)
	if err != nil {
		dc.Log.Error(err, "Could not find leader")
		return err
	}

	if !isLeader {
		return nil
	} else if dc.gormDB != nil {
		dc.Log.Info("Leader elected for migration", "Id", dc.app.ID(), "Address", dc.app.Address())
		dc.Log.Info("Performing migration")
		return dc.gormDB.AutoMigrate(&models.FileMetadata{}, &models.File{}, &models.Metadata{})
	} else {
		return errors.New("GORM connection has not initialised: Connection of type *gorm.DB is nil")
	}
}

func IsLeader(app *app.App) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cli, err := app.Leader(ctx)
	if err != nil {
		return false, err
	}
	var leader *client.NodeInfo
	for leader == nil {
		leader, err = cli.Leader(ctx)
		if err != nil {
			return false, err
		}
	}
	if leader.Address == app.Address() {
		return true, nil
	}
	return false, nil
}

// GetApp returns dqlite-go application helper
func (dc *DatabaseConfig) GetApp() *app.App {
	return dc.app
}

// Close ensures all responsibilites for the node are handled gracefully on exit
func (dc *DatabaseConfig) Close() {
	if dc != nil {
		dc.dqliteDB.Close()
		dc.app.Handover(context.Background())
		dc.app.Close()
	}
}
