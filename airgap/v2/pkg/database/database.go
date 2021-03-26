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

package database

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	v1 "github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/apis/model/v1"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/pkg/models"
	"gorm.io/gorm"
)

type File interface {
	SaveFile(finfo *v1.FileInfo, bs []byte) error
	FetchFile(finfo *v1.FileID) (models.Metadata, error)
}

type Database struct {
	DB  *gorm.DB
	Log logr.Logger
}

func (d *Database) SaveFile(finfo *v1.FileInfo, bs []byte) error {
	// Validating input data
	if finfo == nil || bs == nil {
		return fmt.Errorf("nil arguments received: finfo: %v bs: %v", finfo, bs)
	} else if finfo.GetFileId() == nil {
		return fmt.Errorf("file id struct is nil")
	} else if len(strings.TrimSpace(finfo.GetFileId().GetId())) == 0 && len(strings.TrimSpace(finfo.GetFileId().GetName())) == 0 {
		return fmt.Errorf("file id/name is blank")
	}

	// Create a slice of file metadata models
	var fms []models.FileMetadata
	m := finfo.GetMetadata()
	for k, v := range m {
		fm := models.FileMetadata{
			Key:   k,
			Value: v,
		}
		fms = append(fms, fm)
	}

	// Create metadata along with associations
	metadata := models.Metadata{
		ProvidedId:      finfo.GetFileId().GetId(),
		ProvidedName:    finfo.GetFileId().GetName(),
		Size:            finfo.GetSize(),
		Compression:     finfo.GetCompression(),
		CompressionType: finfo.GetCompressionType(),
		File: models.File{
			Content: bs,
		},
		FileMetadata: fms,
	}
	err := d.DB.Create(&metadata).Error
	if err != nil {
		d.Log.Error(err, "Failed to save model")
		return err
	}

	d.Log.Info(fmt.Sprintf("File of size: %v saved with id: %v", metadata.Size, metadata.FileID))
	return nil
}

func (d *Database) FetchFile(finfo *v1.FileID) (models.Metadata, error) {

	var meta models.Metadata
	var filename string
	var fileid string

	res := d.DB

	if len(strings.TrimSpace(finfo.GetId())) != 0 {

		fileid = strings.TrimSpace(finfo.GetId())
		res = d.DB.Where("provided_id = ?", fileid).Order("created_at desc").Preload("File").First(&meta)

	} else if len(strings.TrimSpace(finfo.GetName())) != 0 {

		filename = strings.TrimSpace(finfo.GetName())
		res = d.DB.Where("provided_name = ?", filename).Order("created_at desc").Preload("File").First(&meta)

	} else {
		return meta, fmt.Errorf("file id/name is blank")
	}
	if res.RowsAffected == 0 {
		er := "No File found with name: " + filename
		return meta, fmt.Errorf(er)
	}
	return meta, nil
}
