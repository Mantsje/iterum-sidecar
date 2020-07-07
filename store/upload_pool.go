package store

import (
	"fmt"
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/prometheus/common/log"
)

// UploadPool is a struct responsible for at most PoolSize files at one time
type UploadPool struct {
	poolSize    int
	workerGroup *sync.WaitGroup
	Input       chan uploadRequest
	Minio       minio.Config
}

type uploadRequest struct {
	descriptor desc.LocalFileDesc
	completed  chan desc.RemoteFileDesc
}

// NewUploadPool creates a new UploadPool instance that will download the files passed on its input
func NewUploadPool(size int, miniocfg minio.Config) UploadPool {
	return UploadPool{
		size,
		&sync.WaitGroup{},
		make(chan uploadRequest, size*20),
		miniocfg,
	}
}

// Size returns the size of the download pool
func (up UploadPool) Size() int {
	return up.poolSize
}

// StartBlocking starts a tracker and the downloading process of each of the files
func (up UploadPool) StartBlocking() {
	log.Infoln("UploadPool starting")

	err := up.Minio.EnsureBucket(up.Minio.TargetBucket, 10)
	util.Ensure(err, fmt.Sprintf("Output bucket '%v' was created successfully", up.Minio.TargetBucket))

	// Spawn poolsize uploader workers
	for w := 0; w < up.poolSize; w++ {
		up.workerGroup.Add(1)
		go func(toUpload <-chan uploadRequest) {
			defer up.workerGroup.Done()
			for request := range toUpload {
				localFileDesc := request.descriptor
				remoteFileDesc, err := up.Minio.PutFile(localFileDesc, false) // Bucket is guaranteed already
				if err != nil {
					log.Errorf("Upload failed due to: '%v'\n", err)
				}
				request.completed <- remoteFileDesc
			}
		}(up.Input)
	}
	log.Infoln("UploadPool awaiting completion")
	up.workerGroup.Wait()
	log.Infoln("UploadPool finishing up")
}

// Start asychronously calls StartBlocking via a Goroutine
func (up UploadPool) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		up.StartBlocking()
	}()
}
