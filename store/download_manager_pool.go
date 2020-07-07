package store

import (
	"sync"
	"time"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/env"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/prometheus/common/log"
)

// DownloadManagerPool is the structure that consumes RemoteFragmentDesc structures and downloads them
type DownloadManagerPool struct {
	ToDownload     chan transmit.Serializable // desc.RemoteFragmentDesc
	Completed      chan transmit.Serializable // desc.LocalFragmentDesc
	pool           DownloadPool
	fragments      int
	strictOrdering bool
}

// NewDownloadManagerPool creates a new downloadmanager and initiates a client of the Minio service
func NewDownloadManagerPool(minio minio.Config, toDownload, completed chan transmit.Serializable) DownloadManagerPool {
	return DownloadManagerPool{
		toDownload,
		completed,
		NewDownloadPool(10, minio),
		0,
		false,
	}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and downloading the associated data
func (dm DownloadManagerPool) StartBlocking() {
	startTime := time.Now()

	log.Infoln("DownloadManagerPool starting")
	var poolGroup sync.WaitGroup
	dm.pool.Start(&poolGroup)
	var downloaderGroup sync.WaitGroup
	for msg := range dm.ToDownload {
		rfd := *msg.(*desc.RemoteFragmentDesc)
		dloader := NewFragmentDownloader(rfd, &dm.pool, dm.Completed, env.DataVolumePath)
		if dm.strictOrdering {
			dloader.StartBlocking()
		} else {
			dloader.Start(&downloaderGroup)
		}
		dm.fragments++
	}
	log.Infoln("DownloadManagerPool awaiting child routines")
	downloaderGroup.Wait()
	close(dm.pool.Input)
	poolGroup.Wait()
	log.Infof("DownloadManagerPool finishing up, (tried to) download(ed) %v fragments", dm.fragments)
	close(dm.Completed)
	log.Infof("DownloadManagerPoolRan for %v", time.Now().Sub(startTime))
}

// Start asychronously calls StartBlocking via a Goroutine
func (dm DownloadManagerPool) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		dm.StartBlocking()
	}()
}
