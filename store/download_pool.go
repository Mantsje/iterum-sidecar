package store

import (
	"fmt"
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/prometheus/common/log"
)

// DownloadPool is a struct responsible for at most PoolSize files at one time
type DownloadPool struct {
	poolSize    int
	workerGroup *sync.WaitGroup
	Input       chan downloadRequest
	Minio       minio.Config
}

type downloadRequest struct {
	descriptor desc.RemoteFileDesc
	completed  chan desc.LocalFileDesc
	folder     string
}

// NewDownloadPool creates a new DownloadPool instance that will download the files passed on its input
func NewDownloadPool(size int, miniocfg minio.Config) DownloadPool {
	return DownloadPool{
		size,
		&sync.WaitGroup{},
		make(chan downloadRequest, size*20),
		miniocfg,
	}
}

// Size returns the size of the download pool
func (dp DownloadPool) Size() int {
	return dp.poolSize
}

// StartBlocking starts a tracker and the downloading process of each of the files
func (dp DownloadPool) StartBlocking() {
	log.Infoln("DownloadPool starting")
	// Spawn poolsize downloader workers
	for w := 0; w < dp.poolSize; w++ {
		dp.workerGroup.Add(1)
		go func(toDownload <-chan downloadRequest) {
			defer dp.workerGroup.Done()
			validBuckets := make(map[string]bool)
			for request := range toDownload {
				remoteFile := request.descriptor
				valid, ok := validBuckets[remoteFile.Bucket]
				localFileDesc, err := dp.Minio.GetFile(remoteFile, request.folder, !(ok && valid))
				if err != nil {
					log.Errorf("Download failed due to: '%v'\n %s", err, fmt.Sprintf("Bucket: '%v', Name: '%v', Folder: '%v'", remoteFile.Bucket, remoteFile.Name, request.folder))
					continue
				}
				validBuckets[remoteFile.Bucket] = true
				request.completed <- localFileDesc
			}
		}(dp.Input)
	}
	log.Infoln("DownloadPool awaiting completion")
	dp.workerGroup.Wait()
	log.Infoln("DownloadPool finishing up")
}

// Start asychronously calls StartBlocking via a Goroutine
func (dp DownloadPool) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		dp.StartBlocking()
	}()
}
