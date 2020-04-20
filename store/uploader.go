package store

import (
	"sync"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/transmit"
	"github.com/minio/minio-go/v6"
	"github.com/prometheus/common/log"
)

// Uploader is a struct responsible for uploading a single fragment to the minio client
type Uploader struct {
	Completed        map[string]string          // maps the name to the LocalPath, "" means unuploaded
	NotifyComplete   chan data.RemoteFileDesc   // Channel to notify uploader of individual file completion
	NotifyManager    chan transmit.Serializable // Channel to notify upload_manager of fragment completion
	UploadDescriptor data.LocalFragmentDesc
	Client           *minio.Client
	Bucket           string
}

// NewUploader creates a new Uploader instance that will upload the passed fragment description
func NewUploader(msg data.LocalFragmentDesc, client *minio.Client, manager chan transmit.Serializable, targetBucket string) Uploader {
	completed := make(map[string]string)
	for _, file := range msg.Files {
		completed[file.Name] = ""
	}

	return Uploader{completed, make(chan data.RemoteFileDesc, len(msg.Files)), manager, msg, client, targetBucket}
}

// IsComplete checks whether all uploads have completed
func (u Uploader) IsComplete() bool {
	for _, val := range u.Completed {
		if val == "" {
			return false
		}
	}
	return true
}

// completionTracker is a function that tracks whether all uploads have completed yet
func (u Uploader) completionTracker(wg *sync.WaitGroup) {
	// Lazy loop and wait until all files have been uploaded
	var files []data.RemoteFileDesc
	for !u.IsComplete() {
		uloadedFile := <-u.NotifyComplete
		if path, ok := u.Completed[uloadedFile.Name]; !ok {
			log.Errorf("Uploaded a file that is not in de Completed map: '%v\n", uloadedFile.Name)
		} else if path != "" {
			log.Errorf("Uploaded a file that is already complete: '%v'\n", uloadedFile.Name)
		} else {
			u.Completed[uloadedFile.Name] = uloadedFile.RemotePath
			files = append(files, uloadedFile)
		}
	}
	rfd := data.RemoteFragmentDesc{Files: files}
	log.Infoln("Fragment uploaded")
	u.NotifyManager <- &rfd
	wg.Done()
}

func (u Uploader) upload(descriptor data.LocalFileDesc, wg *sync.WaitGroup) {
	// Check to see if we already own this bucket
	exists, errBucketExists := u.Client.BucketExists(u.Bucket)
	if errBucketExists != nil {
		log.Errorf("Upload failed due to failure of bucket existence checking: '%v'\n", errBucketExists)
	} else if !exists {
		log.Errorf("Bucket '%v' does not exist, creating...\n", u.Bucket)
		errMakeBucket := u.Client.MakeBucket(u.Bucket, "")
		if errMakeBucket != nil {
			log.Errorf("Failed to create bucket '%v' due to: '%v'\n", u.Bucket, errMakeBucket)
		}
	}
	// Upload the file with FPutObject
	_, err := u.Client.FPutObject(u.Bucket, descriptor.Name, descriptor.LocalPath, putOptions)
	if err != nil {
		log.Errorf("Upload failed due to: '%v'", err)
	} else {
		u.NotifyComplete <- data.RemoteFileDesc{Name: descriptor.Name, RemotePath: descriptor.Name, Bucket: u.Bucket}
	}
	wg.Done()
}

// Start enters an loop that uploads all files via the client in goroutines
func (u Uploader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go u.completionTracker(wg)
	for _, file := range u.UploadDescriptor.Files {
		wg.Add(1)
		go u.upload(file, wg)
	}
}
