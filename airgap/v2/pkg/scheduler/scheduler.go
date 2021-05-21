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

package scheduler

import (
	"fmt"
	"strconv"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/go-co-op/gocron"
	"github.com/go-logr/logr"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/database"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/dqlite"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SchedulerConfig struct {
	CronExpression string
	PurgeBefore    string
	Purge          bool
	Log            logr.Logger
	Fs             *database.Database
	app            *app.App
}

func (sfg *SchedulerConfig) SetApp(app *app.App) {
	sfg.app = app
}

// Schedule creates and returns gocron.scheduler
func (sfg *SchedulerConfig) schedule() *gocron.Scheduler {
	s := gocron.NewScheduler(time.UTC)
	_, err := s.CronWithSeconds(sfg.CronExpression).Do(func() {
		sfg.handler()
	})
	if err != nil {
		sfg.Log.Error(err, "error running job")
	}
	return s
}

// Start starts all job for passed scheduler
func (sfg *SchedulerConfig) Start() {
	s := sfg.schedule()
	s.StartAsync()
}

// handler is jobFuncthat that should be called every time the Job runs
func (sfg *SchedulerConfig) handler() error {
	isLeader, err := dqlite.IsLeader(sfg.app)
	if err != nil {
		return err
	}
	if !isLeader {
		return fmt.Errorf("node is not leader, cannnot run the job")
	}

	sfg.Log.Info("Job", "time", time.Now().Unix(), "diff", sfg.PurgeBefore, "purge", sfg.Purge)

	diff, err := strconv.Atoi(sfg.PurgeBefore)
	if err != nil {
		sfg.Log.Error(err, "integer value required")
	}

	now := time.Now()
	t1 := now.Add(time.Duration(-diff) * time.Second).Unix()
	t := &timestamppb.Timestamp{Seconds: t1}

	fileId, err := sfg.Fs.CleanTombstones(t, sfg.Purge)
	if err != nil {
		sfg.Log.Error(err, "")
	}

	sfg.Log.Info("Fileid", "ids", fileId)
	return nil
}
