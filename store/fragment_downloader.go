package store

import (
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/transmit"
)

// FragmentDownloader is a struct responsible for downloading a single fragment to local disk
type FragmentDownloader struct {
	complete           chan desc.LocalFileDesc // Channel to notify downloader of individual file completion
	pool               *DownloadPool
	notifyManager      chan transmit.Serializable // Channel to notify download_manager of fragment completion
	DownloadDescriptor desc.RemoteFragmentDesc    // The FragmentToDownload
	Folder             string
}

// NewFragmentDownloader creates a new FragmentDownloader instance that will download teh pass fragment description
func NewFragmentDownloader(msg desc.RemoteFragmentDesc, pool *DownloadPool, manager chan transmit.Serializable, targetFolder string) FragmentDownloader {
	return FragmentDownloader{
		make(chan desc.LocalFileDesc, len(msg.Files)),
		pool,
		manager,
		msg,
		targetFolder,
	}
}

// completionTracker is a function that tracks whether all downloads have completed yet
func (d FragmentDownloader) completionTracker(wg *sync.WaitGroup) {
	defer wg.Done()
	// Lazy loop and wait until all files have been downloaded
	var files []desc.LocalFileDesc
	var downloaded int = 0
	for downloaded < len(d.DownloadDescriptor.Files) {
		dloadedFile := <-d.complete
		files = append(files, dloadedFile)
		downloaded++
	}
	lfd := desc.LocalFragmentDesc{Files: files, Metadata: toLocalMetadata(d.DownloadDescriptor.Metadata)}
	d.notifyManager <- &lfd
}

// StartBlocking starts a tracker and the downloading process of each of the files
func (d FragmentDownloader) StartBlocking() {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	// Submit request for each file to be downloaded by a DownloadPool
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for _, file := range d.DownloadDescriptor.Files {
			d.pool.Input <- downloadRequest{
				descriptor: file,
				completed:  d.complete,
				folder:     d.Folder,
			}
		}
	}(wg)
	// Await the completion of those download requests
	go d.completionTracker(wg)
	wg.Wait()
}

// Start asychronously calls StartBlocking via a Goroutine
func (d FragmentDownloader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.StartBlocking()
	}()
}
