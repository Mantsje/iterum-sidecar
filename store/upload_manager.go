package store

import (
	"sync"
	"time"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/sidecar/env/config"
	"github.com/iterum-provenance/sidecar/garbage"

	"github.com/prometheus/common/log"
)

// UploadManager is the structure that consumes LocalFragmentDesc structures and uploads them to minio
type UploadManager struct {
	ToUpload       chan transmit.Serializable // desc.LocalFragmentDesc
	Completed      chan transmit.Serializable // desc.RemoteFragmentDesc
	Minio          minio.Config
	sidecarConfig  *config.Config
	fragments      int
	strictOrdering bool
	fragCollector  garbage.FragmentCollector
}

// NewUploadManager creates a new upload manager and initiates a client of the Minio service
func NewUploadManager(toUpload, uploaded chan transmit.Serializable,
	sidecarConfig *config.Config, collector garbage.FragmentCollector) UploadManager {

	minio := minio.NewMinioConfigFromEnv() // defaults to an upload setup
	if err := minio.Connect(); err != nil {
		log.Fatal(err)
	}
	return UploadManager{
		toUpload,
		uploaded,
		minio,
		sidecarConfig,
		0,
		false,
		collector,
	}
}

// StartBlocking enters an endless loop consuming LocalFragmentDesc and uploading the associated data
func (umanager UploadManager) StartBlocking() {
	var wg sync.WaitGroup
	for {
		msg, ok := <-umanager.ToUpload
		if !ok {
			break
		}
		uloader := NewUploader(*msg.(*desc.LocalFragmentDesc), umanager.Minio, umanager.Completed, umanager.sidecarConfig, umanager.fragCollector)

		if umanager.strictOrdering {
			uloader.StartBlocking()
		} else {
			uloader.Start(&wg)
		}

		umanager.fragments++
	}
	wg.Wait()
	umanager.Stop()
}

// Start asychronously calls StartBlocking via a Goroutine
func (umanager UploadManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		umanager.StartBlocking()
		log.Infof("umanager ran for %v", time.Now().Sub(startTime))
	}()
}

// Stop finishes up and notifies the user of its progress
func (umanager UploadManager) Stop() {
	log.Infof("UploadManager finishing up, (tried to) upload(ed) %v fragments", umanager.fragments)
	close(umanager.Completed)
	close(umanager.fragCollector.Track)
	close(umanager.fragCollector.Collect)
}
