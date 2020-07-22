package store

import (
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/sidecar/env/config"
	"github.com/iterum-provenance/sidecar/garbage"
	"github.com/prometheus/common/log"
)

// FragmentUploader is a struct responsible for downloading a single fragment to local disk
type FragmentUploader struct {
	fileComplete     chan desc.RemoteFileDesc // Channel to notify uploader of individual file completion
	UploadDescriptor desc.LocalFragmentDesc   // The fragment to upload
	pool             *UploadPool
	done             chan transmit.Serializable // Channel to send that the fragment is done uploading
	sidecarConfig    *config.Config
	fragCollector    *garbage.FragmentCollector
}

// NewFragmentUploader creates a new FragmentUploader instance that will download teh pass fragment description
func NewFragmentUploader(msg desc.LocalFragmentDesc, pool *UploadPool, done chan transmit.Serializable,
	sidecarConf *config.Config, collector *garbage.FragmentCollector,
) FragmentUploader {
	return FragmentUploader{
		make(chan desc.RemoteFileDesc, len(msg.Files)),
		msg,
		pool,
		done,
		sidecarConf,
		collector,
	}
}

// completionTracker is a function that tracks whether all downloads have completed yet
func (fu FragmentUploader) completionTracker() {
	// Lazy loop and wait until all files have been downloaded
	var files []desc.RemoteFileDesc
	var uploaded int = 0
	for uploaded < len(fu.UploadDescriptor.Files) {
		uploadedFile := <-fu.fileComplete
		files = append(files, uploadedFile)
		uploaded++
	}
	meta := toRemoteMetadata(fu.sidecarConfig, fu.UploadDescriptor.Metadata)
	meta.FragmentID = desc.NewIterumID() // Generate new ID for the new fragment
	if err := meta.Validate(); err != nil {
		log.Errorln(err)
	}
	rfd := desc.RemoteFragmentDesc{Files: files, Metadata: meta}
	fu.done <- &rfd
}

// StartBlocking starts a tracker and the downloading process of each of the files
func (fu *FragmentUploader) StartBlocking() {
	// The garbage collector should track the local fragment to be uploaded
	// If there is no fragmentID in the desc then generate one
	// (LocalFragmentDesc from users don't need to have an ID on arrival)
	if fu.UploadDescriptor.Metadata.FragmentID == "" {
		fu.UploadDescriptor.Metadata.FragmentID = desc.NewIterumID()
	}
	fu.fragCollector.Track <- &fu.UploadDescriptor

	// Submit request for each file to be uploaded by the UploadPool
	for _, file := range fu.UploadDescriptor.Files {
		fu.pool.Input <- uploadRequest{
			descriptor: file,
			completed:  fu.fileComplete,
		}
	}
	// Await the completion of those upload requests
	fu.completionTracker()

	// Once done uploading the garbage collector can remove the files locally
	fu.fragCollector.Collect <- fu.UploadDescriptor.Metadata.FragmentID
}

// Start asychronously calls StartBlocking via a Goroutine
func (fu FragmentUploader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fu.StartBlocking()
	}()
}
