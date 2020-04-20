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

// UploadManager is the structure that consumes LocalFragmentDesc structures and uploads them to minio
type UploadManager struct {
	ToUpload  chan transmit.Serializable // data.LocalFragmentDesc
	Completed chan transmit.Serializable // data.RemoteFragmentDesc
	Client    *minio.Client
	Bucket    string
}

// NewUploadManager creates a new upload manager and initiates a client of the Minio service
func NewUploadManager(toUpload, completed chan transmit.Serializable) UploadManager {
	endpoint := os.Getenv("MINIO_URL")
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")
	useSSL, sslErr := strconv.ParseBool(os.Getenv("MINIO_USE_SSL"))
	if sslErr != nil {
		useSSL = false
	}

	targetBucket := os.Getenv("MINIO_OUTPUT_BUCKET")

	// Initialize minio client object.
	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}

	return UploadManager{toUpload, completed, client, targetBucket}
}

// StartBlocking enters an endless loop consuming LocalFragmentDesc and uploading the associated data
func (um UploadManager) StartBlocking() {
	var wg sync.WaitGroup
	for msg := range um.ToUpload {
		wg.Add(1)
		uloader := NewUploader(*msg.(*data.LocalFragmentDesc), um.Client, um.Completed, um.Bucket)
		go uloader.Start(&wg)
	}
	wg.Wait()
}

// Start asychronously calls StartBlocking via a Goroutine
func (um UploadManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		um.StartBlocking()
	}()
}
