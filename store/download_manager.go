package store

import (
	"os"
	"strconv"
	"sync"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/transmit"
	"github.com/minio/minio-go/v6"
	"github.com/prometheus/common/log"
)

// DownloadManager is the structure that consumes RemoteFragmentDesc structures and downloads them
type DownloadManager struct {
	ToDownload chan transmit.Serializable // data.RemoteFragmentDesc
	Completed  chan transmit.Serializable // data.LocalFragmentDesc
	Client     *minio.Client
}

// NewDownloadManager creates a new downloadmanager and initiates a client of the Minio service
func NewDownloadManager(toDownload, completed chan transmit.Serializable) DownloadManager {
	endpoint := os.Getenv("MINIO_URL")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")
	useSSL, sslErr := strconv.ParseBool(os.Getenv("MINIO_USE_SSL"))
	if sslErr != nil {
		useSSL = false
	}

	// Initialize minio client object.
	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	return DownloadManager{toDownload, completed, client}
}

// StartBlocking enters an endless loop consuming RemoteFragmentDescs and downloading the associated data
func (dm DownloadManager) StartBlocking() {
	var wg sync.WaitGroup
	for msg := range dm.ToDownload {
		wg.Add(1)
		dloader := NewDownloader(*msg.(*data.RemoteFragmentDesc), dm.Client, dm.Completed, "./")
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
