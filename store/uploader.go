package store

import (
	"sync"

	"github.com/prometheus/common/log"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/transmit"
)

// Uploader is a struct responsible for uploading a single fragment to the minio client
type Uploader struct {
	Completed        map[string]string          // maps the name to the LocalPath, "" means unuploaded
	NotifyComplete   chan desc.RemoteFileDesc   // Channel to notify uploader of individual file completion
	NotifyManager    chan transmit.Serializable // Channel to notify upload_manager of fragment completion
	UploadDescriptor desc.LocalFragmentDesc
	Minio            minio.Config
}

// NewUploader creates a new Uploader instance that will upload the passed fragment description
func NewUploader(msg desc.LocalFragmentDesc, minio minio.Config, manager chan transmit.Serializable) Uploader {
	completed := make(map[string]string)
	for _, file := range msg.Files {
		completed[file.Name] = ""
	}

	return Uploader{completed, make(chan desc.RemoteFileDesc, len(msg.Files)), manager, msg, minio}
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
	defer wg.Done()
	// Lazy loop and wait until all files have been uploaded
	var files []desc.RemoteFileDesc
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
	rfd := desc.RemoteFragmentDesc{Files: files, Metadata: toRemoteMetadata(u.UploadDescriptor.Metadata)}
	u.NotifyManager <- &rfd
}

func (u Uploader) upload(descriptor desc.LocalFileDesc, wg *sync.WaitGroup) {
	defer wg.Done()
	remoteFile, err := u.Minio.PutFile(descriptor)
	if err != nil {
		log.Errorf("Upload failed due to: '%v'\n", err)
	}
	u.NotifyComplete <- remoteFile
}

// StartBlocking starts a tracker and the uploading process of each of the files
func (u Uploader) StartBlocking() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go u.completionTracker(wg)
	for _, file := range u.UploadDescriptor.Files {
		wg.Add(1)
		go u.upload(file, wg)
	}
	wg.Wait()
}

// Start asychronously calls StartBlocking via a Goroutine
func (u Uploader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		u.StartBlocking()
	}()
}
