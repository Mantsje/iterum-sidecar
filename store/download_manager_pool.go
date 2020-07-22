package store

import (
	"sync"
	"time"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/prometheus/common/log"
)

// DownloadManagerPool is the structure that consumes RemoteFragmentDesc structures and downloads them
type DownloadManagerPool struct {
	ToDownload     chan transmit.Serializable // desc.RemoteFragmentDesc
	Completed      chan transmit.Serializable // desc.LocalFragmentDesc
	pool           DownloadPool
	targetFolder   string
	fragments      int
	strictOrdering bool
}

// NewDownloadManagerPool creates a new downloadmanager and initiates a client of the Minio service
func NewDownloadManagerPool(toDownload, completed chan transmit.Serializable, folder string) DownloadManagerPool {
	minio := minio.NewMinioConfigFromEnv() // defaults to an upload setup
	minio.TargetBucket = "INVALID"         // adjust such that the target output is unusable
	if err := minio.Connect(); err != nil {
		log.Fatal(err)
	}
	return DownloadManagerPool{
		toDownload,
		completed,
		NewDownloadPool(10, minio),
		folder,
		0,
		false,
	}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and downloading the associated data
func (dm DownloadManagerPool) StartBlocking() {
	log.Infoln("DownloadManagerPool starting")
	var poolGroup sync.WaitGroup
	dm.pool.Start(&poolGroup)
	var downloaderGroup sync.WaitGroup
	for msg := range dm.ToDownload {
		rfd := *msg.(*desc.RemoteFragmentDesc)
		dloader := NewFragmentDownloader(rfd, &dm.pool, dm.Completed, dm.targetFolder)
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
}

// Start asychronously calls StartBlocking via a Goroutine
func (dm DownloadManagerPool) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		dm.StartBlocking()
		log.Infof("dmanagerpool ran for %v", time.Now().Sub(startTime))
	}()
}
