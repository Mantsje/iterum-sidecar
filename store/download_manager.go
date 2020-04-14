package store

import (
	"os"
	"strconv"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/minio/minio-go/v6"
	"github.com/prometheus/common/log"
)

// DownloadManager is the structure that consumes RemoteFragmentDesc structures and downloads them
type DownloadManager struct {
	ToDownload chan data.RemoteFragmentDesc
	Completed  chan data.LocalFragmentDesc
	Client     *minio.Client
}

// NewDownloadManager creates a new downloadmanager and initiates a client of the Minio service
func NewDownloadManager(toDownload chan data.RemoteFragmentDesc, completed chan data.LocalFragmentDesc) DownloadManager {
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
	for {
		msg := <-dm.ToDownload
		dloader := NewDownloader(msg, dm.Client, dm.Completed, "./")
		go dloader.Start()
	}
}

// Start asychronously calls StartBlocking via a Goroutine
func (dm DownloadManager) Start() {
	go dm.StartBlocking()
}
