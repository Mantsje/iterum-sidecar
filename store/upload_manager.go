package store

import (
	"sync"

	// "github.com/minio/minio-go/v6"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"

	"github.com/iterum-provenance/iterum-go/transmit"
)

// UploadManager is the structure that consumes LocalFragmentDesc structures and uploads them to minio
type UploadManager struct {
	ToUpload  chan transmit.Serializable // desc.LocalFragmentDesc
	Completed chan transmit.Serializable // desc.RemoteFragmentDesc
	Minio     minio.Config
}

// NewUploadManager creates a new upload manager and initiates a client of the Minio service
func NewUploadManager(minio minio.Config, toUpload, completed chan transmit.Serializable) UploadManager {

	return UploadManager{toUpload, completed, minio}
}

// StartBlocking enters an endless loop consuming LocalFragmentDesc and uploading the associated data
func (um UploadManager) StartBlocking() {
	var wg sync.WaitGroup
	for msg := range um.ToUpload {
		wg.Add(1)
		uloader := NewUploader(*msg.(*desc.LocalFragmentDesc), um.Minio, um.Completed)
		go uloader.Start(&wg)
	}
	wg.Wait()
}

// Start asychronously calls StartBlocking via a Goroutine
func (um UploadManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		um.StartBlocking()
	}()
}
