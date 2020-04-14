package store

import (
	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/minio/minio-go/v6"
	"github.com/prometheus/common/log"
)

// Downloader is a struct responsible for downloading a single fragment to local disk
type Downloader struct {
	Completed          map[string]string           // maps the name to the LocalPath, "" means undownloaded
	NotifyComplete     chan data.LocalFileDesc     // Channel to notify downloader of individual file completion
	NotifyManager      chan data.LocalFragmentDesc // Channel to notify download_manager of fragment completion
	DownloadDescriptor data.RemoteFragmentDesc
	Client             *minio.Client
	Folder             string
}

// NewDownloader creates a new Downloader instance that will download teh pass fragment description
func NewDownloader(msg data.RemoteFragmentDesc, client *minio.Client, manager chan data.LocalFragmentDesc, targetFolder string) Downloader {
	completed := make(map[string]string)
	for _, file := range msg.Files {
		completed[file.Name] = ""
	}

	return Downloader{completed, make(chan data.LocalFileDesc, len(msg.Files)), manager, msg, client, targetFolder}
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
func (d Downloader) completionTracker() {
	// Lazy loop and wait until all files have been downloaded
	var files []data.LocalFileDesc
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
	lfd := data.LocalFragmentDesc{Files: files}
	log.Infoln("Fragment downloaded")
	d.NotifyManager <- lfd
}

func (d Downloader) download(descriptor data.RemoteFileDesc) {
	// Check to see if we already own this bucket
	exists, errBucketExists := d.Client.BucketExists(descriptor.Bucket)
	if errBucketExists != nil {
		log.Errorf("Download failed due to failure of bucket existence checking: '%v'\n", errBucketExists)
	} else if !exists {
		log.Errorf("Download failed bucket '%v' does not exist\n", descriptor.Bucket)
	} else {
		localFilePath := descriptor.ToLocalPath(d.Folder)
		err := d.Client.FGetObject(descriptor.Bucket, descriptor.RemotePath, localFilePath, getOptions)
		if err != nil {
			log.Errorf("Download failed due to: '%v'", err)
		}
		d.NotifyComplete <- data.LocalFileDesc{Name: descriptor.Name, LocalPath: localFilePath}
	}
}

// Start enters an loop that downloads all files via the client in goroutines
func (d Downloader) Start() {
	go d.completionTracker()
	for _, file := range d.DownloadDescriptor.Files {
		go d.download(file)
	}
}
