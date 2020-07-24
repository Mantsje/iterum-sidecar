package store

import (
	"sync"
	"time"

	"github.com/prometheus/common/log"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
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
func NewDownloadManager(folder string, toDownload, downloaded chan transmit.Serializable) DownloadManager {
	minio := minio.NewMinioConfigFromEnv() // defaults to an upload setup
	minio.TargetBucket = "INVALID"         // adjust such that the target output is unusable
	if err := minio.Connect(); err != nil {
		log.Fatal(err)
	}
	return DownloadManager{toDownload, downloaded, minio, folder, 0, false}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and downloading the associated data
func (dmanager DownloadManager) StartBlocking() {
	var wg sync.WaitGroup
	for {
		msg, ok := <-dmanager.ToDownload
		if !ok {
			break
		}
		dloader := NewDownloader(*msg.(*desc.RemoteFragmentDesc), dmanager.Minio, dmanager.Completed, dmanager.TargetFolder)
		if dmanager.strictOrdering {
			dloader.StartBlocking()
		} else {
			dloader.Start(&wg)
		}
		dmanager.fragments++
	}
	wg.Wait()
	dmanager.Stop()
}

// Start asychronously calls StartBlocking via a Goroutine
func (dmanager DownloadManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		dmanager.StartBlocking()
		log.Infof("dmanager ran for %v", time.Now().Sub(startTime))
	}()
}

// Stop finishes up and notifies the user of its progress
func (dmanager DownloadManager) Stop() {
	log.Infof("DownloadManager finishing up, (tried to) download(ed) %v fragments", dmanager.fragments)
	close(dmanager.Completed)
}
