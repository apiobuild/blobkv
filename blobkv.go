package blobkv

import (
	"github.com/apiobuild/blobkv/config"
	"github.com/apiobuild/blobkv/gcs"
	"github.com/apiobuild/blobkv/model"
)

// GetStoreInterface ...
func GetStoreInterface(backend model.Backend, conf config.Storage) (i model.I, err error) {
	switch backend {
	case model.GCS:
		if i, err = gcs.NewClient(conf); err != nil {
			return
		}
	}
	return
}
