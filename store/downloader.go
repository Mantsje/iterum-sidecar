package store

import (
	"fmt"
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/prometheus/common/log"
)

// Downloader is a struct responsible for downloading a single fragment to local disk
type Downloader struct {
	Completed          map[string]string          // maps the name to the LocalPath, "" means undownloaded
	NotifyComplete     chan desc.LocalFileDesc    // Channel to notify downloader of individual file completion
	NotifyManager      chan transmit.Serializable // Channel to notify download_manager of fragment completion
	DownloadDescriptor desc.RemoteFragmentDesc
	Minio              minio.Config
	Folder             string
}

// NewDownloader creates a new Downloader instance that will download teh pass fragment description
func NewDownloader(msg desc.RemoteFragmentDesc, minio minio.Config, manager chan transmit.Serializable, targetFolder string) Downloader {
	completed := make(map[string]string)
	for _, file := range msg.Files {
		completed[file.Name] = ""
	}

	return Downloader{completed, make(chan desc.LocalFileDesc, len(msg.Files)), manager, msg, minio, targetFolder}
}

// IsComplete checks whether all downloads have completed
func (d Downloader) IsComplete() bool {
	for _, val := range d.Completed {
		if val == "" {
			return false
		}
	}
	return true
}

// completionTracker is a function that tracks whether all downloads have completed yet
func (d Downloader) completionTracker(wg *sync.WaitGroup) {
	defer wg.Done()
	// Lazy loop and wait until all files have been downloaded
	var files []desc.LocalFileDesc
	for !d.IsComplete() {
		dloadedFile := <-d.NotifyComplete
		if path, ok := d.Completed[dloadedFile.Name]; !ok {
			log.Errorf("Downloaded a file that is not in de Completed map: '%v\n", dloadedFile.Name)
		} else if path != "" {
			log.Errorf("Downloaded a file that is already complete: '%v'\n", dloadedFile.Name)
		} else {
			d.Completed[dloadedFile.Name] = dloadedFile.LocalPath
			files = append(files, dloadedFile)
		}
	}
	lfd := desc.LocalFragmentDesc{Files: files, Metadata: toLocalMetadata(d.DownloadDescriptor.Metadata)}
	d.NotifyManager <- &lfd
}

func (d Downloader) download(descriptor desc.RemoteFileDesc, wg *sync.WaitGroup) {
	defer wg.Done()
	localFileDesc, err := d.Minio.GetFile(descriptor, d.Folder, true)
	if err != nil {
		log.Errorf("Download failed due to: '%v'\n %s", err, fmt.Sprintf("Bucket: '%v', Name: '%v', Folder: '%v'", descriptor.Bucket, descriptor.Name, d.Folder))
		return
	}
	d.NotifyComplete <- localFileDesc
}

// StartBlocking starts a tracker and the downloading process of each of the files
func (d Downloader) StartBlocking() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go d.completionTracker(wg)
	for _, file := range d.DownloadDescriptor.Files {
		wg.Add(1)
		go d.download(file, wg)
	}
	wg.Wait()
}

// Start asychronously calls StartBlocking via a Goroutine
func (d Downloader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.StartBlocking()
	}()
}
