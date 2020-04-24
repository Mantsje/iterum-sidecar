package store

import (
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/env"
	"github.com/iterum-provenance/iterum-go/minio"

	"github.com/iterum-provenance/iterum-go/transmit"
)

// DownloadManager is the structure that consumes RemoteFragmentDesc structures and downloads them
type DownloadManager struct {
	ToDownload chan transmit.Serializable // desc.RemoteFragmentDesc
	Completed  chan transmit.Serializable // desc.LocalFragmentDesc
	Minio      minio.Config
}

// NewDownloadManager creates a new downloadmanager and initiates a client of the Minio service
func NewDownloadManager(minio minio.Config, toDownload, completed chan transmit.Serializable) DownloadManager {
	return DownloadManager{toDownload, completed, minio}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and downloading the associated data
func (dm DownloadManager) StartBlocking() {
	var wg sync.WaitGroup
	for msg := range dm.ToDownload {
		wg.Add(1)
		dloader := NewDownloader(*msg.(*desc.RemoteFragmentDesc), dm.Minio, dm.Completed, env.DataVolumePath)
		go dloader.Start(&wg)
	}
	wg.Wait()
}

// Start asychronously calls StartBlocking via a Goroutine
func (dm DownloadManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		dm.StartBlocking()
	}()
}
