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

package test

import (
	"os"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	v1 "github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/apis/model/v1"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/database"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/models"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestSaveFile(t *testing.T) {
	var log logr.Logger
	zapLog, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initialize zapr, due to error: %v", err)
	}
	log = zapr.NewLogger(zapLog)

	defer os.Remove("test.db")

	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Couldn't create sqlite connection")
	}

	//Perform migrations
	db.AutoMigrate(&models.FileMetadata{}, &models.File{}, &models.Metadata{})

	bs := make([]byte, 1024)
	finfo := &v1.FileInfo{
		FileId: &v1.FileID{
			Data: &v1.FileID_Name{
				Name: "test-file",
			},
		},
		Size:            1024,
		Compression:     true,
		CompressionType: "gzip",
		Metadata: map[string]string{
			"Key1": "Value1",
			"Key2": "Value2",
		},
	}

	database := &database.Database{
		DB:  db,
		Log: log,
	}

	dbErr := database.SaveFile(finfo, bs)
	if dbErr != nil {
		t.Fatalf("Couldn't save file due to:%v", dbErr)
	}

	m := &models.Metadata{}
	db.Preload(clause.Associations).First(&m)

	if m.ProvidedName != "test-file" {
		t.Fatalf("File name is incorrect: %v", m.ProvidedName)
	}

	if m.Size != 1024 {
		t.Fatalf("File size is incorrect: %v", m.Size)
	}

	if len(m.FileMetadata) != 2 {
		t.Fatalf("File metadata count is incorrect: %v", len(m.FileMetadata))
	}

}

func TestSaveFileInputValidation(t *testing.T) {
	database := &database.Database{}

	// file info is nil
	dbErr := database.SaveFile(nil, make([]byte, 1))
	if dbErr == nil {
		t.Fatalf("Save method allows nil file info")
	}

	// byte slice is nil
	finfo := &v1.FileInfo{
		FileId: &v1.FileID{
			Data: &v1.FileID_Name{
				Name: "test-file",
			},
		},
		Size:            1024,
		Compression:     true,
		CompressionType: "gzip",
		Metadata: map[string]string{
			"Key1": "Value1",
			"Key2": "Value2",
		},
	}
	dbErr = database.SaveFile(finfo, nil)
	if dbErr == nil {
		t.Fatalf("Save method allows nil byte slice")
	}

	// white space in name
	finfo = &v1.FileInfo{
		FileId: &v1.FileID{
			Data: &v1.FileID_Name{
				Name: " ",
			},
		},
		Size:            1024,
		Compression:     true,
		CompressionType: "gzip",
		Metadata: map[string]string{
			"Key1": "Value1",
			"Key2": "Value2",
		},
	}
	dbErr = database.SaveFile(finfo, make([]byte, 1))
	if dbErr == nil {
		t.Fatalf("Save method allows names with only whitespace")
	}

}

func TestDownloadFileFn(t *testing.T) {

	var log logr.Logger
	zapLog, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initialize zapr, due to error: %v", err)
	}
	log = zapr.NewLogger(zapLog)

	defer os.Remove("test.db")

	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Couldn't create sqlite connection")
	}

	//Perform migrations
	db.AutoMigrate(&models.FileMetadata{}, &models.File{}, &models.Metadata{})

	database := &database.Database{
		DB:  db,
		Log: log,
	}
	bs := make([]byte, 1024)
	finfo := &v1.FileInfo{
		FileId: &v1.FileID{
			Data: &v1.FileID_Name{
				Name: "test-file",
			},
		},
		Size:            1024,
		Compression:     true,
		CompressionType: "gzip",
		Metadata: map[string]string{
			"Key1": "Value1",
			"Key2": "Value2",
		},
	}
	dbErr := database.SaveFile(finfo, bs)
	if dbErr != nil {
		t.Fatalf("Couldn't save file due to:%v", dbErr)
	}

	// Fetch `test-file`
	fName := &v1.FileID{
		Data: &v1.FileID_Name{
			Name: "test-file",
		},
	}

	metadata, err := database.DownloadFile(fName)
	if err != nil {
		t.Fatalf("Error occured while fetching fiie from DB: %v ", dbErr)
	}

	if metadata.ProvidedName != finfo.FileId.GetName() {
		t.Fatalf("Downloaded file name: %v  and Uploaded file Name: %v dosen't match.", metadata.ProvidedName, finfo.FileId.GetName())
	} else {
		t.Logf("Downloaded file name: %v  and Uploaded file Name: %v Matched.", metadata.ProvidedName, finfo.FileId.GetName())
	}
	if metadata.Size != finfo.Size {
		t.Fatalf("Downloaded file size: %v  and Uploaded file size: %v dosen't match.", metadata.Size, finfo.Size)
	} else {
		t.Logf("Downloaded file size: %v  and Uploaded file size: %v Matched.", metadata.Size, finfo.Size)
	}

	// Non Existing File
	t.Log("\n Downloading non existing file")
	fName = &v1.FileID{
		Data: &v1.FileID_Name{
			Name: "file.tz",
		},
	}
	_, dbErr = database.DownloadFile(fName)
	if dbErr == nil {
		t.Fatalf("Download Method send non existing file [Some other file must be fetched]")
	}
}

func TestDownloadFileInputValidation(t *testing.T) {
	var log logr.Logger
	zapLog, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to initialize zapr, due to error: %v", err)
	}
	log = zapr.NewLogger(zapLog)

	defer os.Remove("test.db")

	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Couldn't create sqlite connection")
	}

	//Perform migrations
	db.AutoMigrate(&models.FileMetadata{}, &models.File{}, &models.Metadata{})

	database := &database.Database{
		DB:  db,
		Log: log,
	}

	// Empty name
	fName := &v1.FileID{
		Data: &v1.FileID_Name{
			Name: "     ",
		},
	}

	_, dbErr := database.DownloadFile(fName)
	if dbErr == nil {
		t.Fatalf("Download Method Allows name with only whitespaces.")
	}

	// Empty Id
	fId := &v1.FileID{
		Data: &v1.FileID_Name{
			Name: "     ",
		},
	}

	_, dbErr = database.DownloadFile(fId)
	if dbErr == nil {
		t.Fatalf("Download Method Allows Id with only whitespaces.")
	}
}
