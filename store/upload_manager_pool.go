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

// UploadManagerPool is the structure that consumes LocalFragmentDesc structures and uploads them
type UploadManagerPool struct {
	ToUpload       chan transmit.Serializable // desc.LocalFragmentDesc
	Completed      chan transmit.Serializable // desc.RemoteFragmentDesc
	pool           UploadPool
	sidecarConfig  *config.Config
	fragCollector  *garbage.FragmentCollector
	fragments      int
	strictOrdering bool
}

// NewUploadManagerPool creates a new uploaumanager and initiates a client of the Minio service
func NewUploadManagerPool(minio minio.Config, toUpload, completed chan transmit.Serializable,
	sidecarConfig *config.Config, collector *garbage.FragmentCollector,
) UploadManagerPool {
	return UploadManagerPool{
		toUpload,
		completed,
		NewUploadPool(10, minio),
		sidecarConfig,
		collector,
		0,
		false,
	}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and uploading the associated data
func (um UploadManagerPool) StartBlocking() {
	startTime := time.Now()
	log.Infoln("UploadManagerPool starting")
	var poolGroup sync.WaitGroup
	um.pool.Start(&poolGroup)

	var uploaderGroup sync.WaitGroup
	for msg := range um.ToUpload {
		lfd := *msg.(*desc.LocalFragmentDesc)
		dloader := NewFragmentUploader(lfd, &um.pool, um.Completed, um.sidecarConfig, um.fragCollector)
		if um.strictOrdering {
			dloader.StartBlocking()
		} else {
			dloader.Start(&uploaderGroup)
		}
		um.fragments++
	}

	log.Infoln("UploadManagerPool awaiting child routines")
	uploaderGroup.Wait()
	close(um.pool.Input)
	poolGroup.Wait()

	log.Infof("UploadManagerPool finishing up, (tried to) upload(ed) %v fragments", um.fragments)
	close(um.Completed)
	close(um.fragCollector.Track)
	close(um.fragCollector.Collect)
	log.Infof("UploadManagerPoolRan for %v", time.Now().Sub(startTime))
}

// Start asychronously calls StartBlocking via a Goroutine
func (um UploadManagerPool) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		um.StartBlocking()
	}()
}
