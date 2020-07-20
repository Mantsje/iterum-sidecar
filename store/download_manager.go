package store

import (
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/prometheus/common/log"

	"github.com/iterum-provenance/iterum-go/transmit"
)

// DownloadManager is the structure that consumes RemoteFragmentDesc structures and downloads them
type DownloadManager struct {
	ToDownload     chan transmit.Serializable // desc.RemoteFragmentDesc
	Completed      chan transmit.Serializable // desc.LocalFragmentDesc
	Minio          minio.Config
	TargetFolder   string
	fragments      int
	strictOrdering bool
}

// NewDownloadManager creates a new downloadmanager and initiates a client of the Minio service
func NewDownloadManager(minio minio.Config, folder string, toDownload, completed chan transmit.Serializable) DownloadManager {
	return DownloadManager{toDownload, completed, minio, folder, 0, false}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and downloading the associated data
func (dm DownloadManager) StartBlocking() {
	var wg sync.WaitGroup
	for {
		msg, ok := <-dm.ToDownload
		if !ok {
			break
		}
		dloader := NewDownloader(*msg.(*desc.RemoteFragmentDesc), dm.Minio, dm.Completed, dm.TargetFolder)
		if dm.strictOrdering {
			dloader.StartBlocking()
		} else {
			dloader.Start(&wg)
		}
		dm.fragments++
	}
	wg.Wait()
	dm.Stop()
}

// Start asychronously calls StartBlocking via a Goroutine
func (dm DownloadManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		dm.StartBlocking()
	}()
}

// Stop finishes up and notifies the user of its progress
func (dm DownloadManager) Stop() {
	log.Infof("DownloadManager finishing up, (tried to) download(ed) %v fragments", dm.fragments)
	close(dm.Completed)
}
