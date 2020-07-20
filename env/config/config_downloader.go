package config

import (
	"os"
	"sync"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/process"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/prometheus/common/log"
)

// Downloader is the structure responsible for downloading
// the config files used by the fragmenter to properly fragment data files
type Downloader struct {
	toDownload []string
	Config     *Config
	Minio      minio.Config
}

// NewDownloader instantiates a new config downloader without starting it
func NewDownloader(config *Config, minio minio.Config) Downloader {
	return Downloader{
		toDownload: []string{},
		Config:     config,
		Minio:      minio,
	}
}

func (cd Downloader) downloadConfigFile(file string) (localFile desc.LocalFileDesc, err error) {
	defer util.ReturnErrOnPanic(&err)()
	return cd.Minio.GetConfigFile(file)
}

func (cd *Downloader) findFilesToDownload() {
	files := cd.Minio.ListConfigFiles()
	log.Infoln("Printing ListConfigFiles results")
	log.Infoln(files)
	cd.toDownload = cd.Config.ReturnMatchingFiles(files)
}

// StartBlocking starts the process of downloading the config files
func (cd *Downloader) StartBlocking() {
	if cd.Config == nil || cd.Config.ConfigFiles == nil {
		log.Infoln("No config(-files) submitted to sidecar, so no files to download")
		return
	}

	cd.findFilesToDownload()

	log.Infof("Starting to download %v config files", len(cd.toDownload))
	wg := &sync.WaitGroup{}
	// Ensure that the directory exists
	err := os.MkdirAll(process.ConfigPath, os.ModePerm)
	if err != nil {
		log.Errorln(err)
	}

	// Start the downloading of each config file
	for _, file := range cd.toDownload {
		wg.Add(1)
		go func(f string) {
			_, err := cd.downloadConfigFile(f)
			if err != nil {
				log.Errorf("Could not download config file due to '%v'", err)
			}
			wg.Done()
		}(file)
	}
	// Wait for the downloading to finish
	wg.Wait()
	log.Infof("Finished downloading config files, config.Downloader finishing up")
}

// Start is an asyncrhonous alternative to StartBlocking by spawning a goroutine
func (cd *Downloader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		cd.StartBlocking()
	}()
}
