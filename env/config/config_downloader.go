package config

import (
	"os"
	"sync"
	"time"

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
func NewDownloader(config *Config) Downloader {
	minio := minio.NewMinioConfigFromEnv() // defaults to an upload setup
	minio.TargetBucket = "INVALID"         // adjust such that the target output is unusable
	if err := minio.Connect(); err != nil {
		log.Fatal(err)
	}
	return Downloader{
		toDownload: []string{},
		Config:     config,
		Minio:      minio,
	}
}

func (cfgdownloader Downloader) downloadConfigFile(file string) (localFile desc.LocalFileDesc, err error) {
	defer util.ReturnErrOnPanic(&err)()
	return cfgdownloader.Minio.GetConfigFile(file)
}

func (cfgdownloader *Downloader) findFilesToDownload() {
	files := cfgdownloader.Minio.ListConfigFiles()
	log.Infoln("Printing ListConfigFiles results")
	log.Infoln(files)
	cfgdownloader.toDownload = cfgdownloader.Config.ReturnMatchingFiles(files)
}

// StartBlocking starts the process of downloading the config files
func (cfgdownloader *Downloader) StartBlocking() {
	if cfgdownloader.Config == nil || cfgdownloader.Config.ConfigFiles == nil {
		log.Infoln("No config(-files) submitted to sidecar, so no files to download")
		return
	}

	cfgdownloader.findFilesToDownload()

	log.Infof("Starting to download %v config files", len(cfgdownloader.toDownload))
	wg := &sync.WaitGroup{}
	// Ensure that the directory exists
	err := os.MkdirAll(process.ConfigPath, os.ModePerm)
	if err != nil {
		log.Errorln(err)
	}

	// Start the downloading of each config file
	for _, file := range cfgdownloader.toDownload {
		wg.Add(1)
		go func(f string) {
			_, err := cfgdownloader.downloadConfigFile(f)
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
func (cfgdownloader *Downloader) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := time.Now()
		cfgdownloader.StartBlocking()
		log.Infof("cfgdownloader ran for %v", time.Now().Sub(startTime))
	}()
}
