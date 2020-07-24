package main

import (
	"sync"
	"time"

	"github.com/iterum-provenance/iterum-go/lineage"
	"github.com/iterum-provenance/iterum-go/manager"
	mq "github.com/iterum-provenance/iterum-go/messageq"
	"github.com/iterum-provenance/iterum-go/process"
	"github.com/iterum-provenance/iterum-go/socket"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/iterum-provenance/sidecar/env"
	"github.com/iterum-provenance/sidecar/env/config"
	"github.com/iterum-provenance/sidecar/garbage"
	"github.com/iterum-provenance/sidecar/handler"
	"github.com/iterum-provenance/sidecar/store"
	"github.com/prometheus/common/log"
)

func main() {
	// log.Base().SetLevel("Debug")
	// log.Base().SetLevel("Info")
	startTime := time.Now()
	var wg sync.WaitGroup

	// Pass remote fragments from the message queue listener to the downloader
	mqDownloaderBridgeBufferSize := 10
	mqDownloaderBridge := make(chan transmit.Serializable, mqDownloaderBridgeBufferSize)

	// After downloading messages, pass the local fragment descriptions on to the transformation
	downloaderSocketBridgeBufferSize := 10
	downloaderSocketBridge := make(chan transmit.Serializable, downloaderSocketBridgeBufferSize)

	// After the transformation is done, pass the output local fragment description to the uploader
	socketUploaderBridgeBufferSize := 10
	socketUploaderBridge := make(chan transmit.Serializable, socketUploaderBridgeBufferSize)

	// After uploading pass the remote fragment description on to the message queue publisher
	uploaderMqBridgeBufferSize := 10
	uploaderMqBridge := make(chan transmit.Serializable, uploaderMqBridgeBufferSize)

	// Acknowledge consumed messages only after they have completely been processed
	socketAcknowledgerBridgeBufferSize := 10
	socketAcknowledgerBridge := make(chan transmit.Serializable, socketAcknowledgerBridgeBufferSize)

	// Post lineage information for each processed fragment
	mqLineageBridgeBufferSize := 10
	mqLineageBridge := make(chan transmit.Serializable, mqLineageBridgeBufferSize)

	// MessageQueue setup
	brokerURL := mq.BrokerURL
	outputQueue := mq.OutputQueue
	inputQueue := mq.InputQueue
	prefetchCount := mq.PrefetchCount

	// ######################## ######################## ######################## \\

	// Delete fragments from the shared volume once we're done processing them
	fragCollector := garbage.NewFragmentCollector()
	fragCollector.Start(&wg)

	// Consume incoming messages from the message queue
	mqListener, err := mq.NewListener(mqDownloaderBridge, socketAcknowledgerBridge, brokerURL, inputQueue, prefetchCount)
	util.Ensure(err, "MessageQueue listener succesfully created and listening")
	mqListener.Start(&wg)

	// Associated file downloader
	downloadManager := store.NewDownloadManager(process.DataVolumePath, mqDownloaderBridge, downloaderSocketBridge)
	downloadManager.Start(&wg)

	// Pass local fragment description to transformation
	toSocketFile := env.TransformationStepInputSocket
	outboundFragmentHandler := handler.SendFileHandler(fragCollector)
	toSocket, err := socket.NewSocket(toSocketFile, downloaderSocketBridge, outboundFragmentHandler)
	util.Ensure(err, "OUtbound socket towards transformation succesfully opened and listening")
	toSocket.Start(&wg)

	// Receive processed fragment descriptions
	fromSocketFile := env.TransformationStepOutputSocket
	inboundFragmentHandler := handler.ProcessedFileHandler(socketAcknowledgerBridge, fragCollector)
	fromSocket, err := socket.NewSocket(fromSocketFile, socketUploaderBridge, inboundFragmentHandler)
	util.Ensure(err, "Inbound socket from transformation succesfully opened and listening")
	fromSocket.Start(&wg)

	// Upload processed fragment data
	uploadManager := store.NewUploadManager(socketUploaderBridge, uploaderMqBridge, env.SidecarConfig, fragCollector)
	uploadManager.Start(&wg)

	// Post remote fragment descriptions
	mqSender, err := mq.NewSender(uploaderMqBridge, mqLineageBridge, brokerURL, outputQueue)
	util.Ensure(err, "MessageQueue sender succesfully created and listening")
	mqSender.Start(&wg)

	// Check regularly whether previous steps in the pipeline have completed
	usChecker := manager.NewUpstreamChecker(manager.URL, process.PipelineHash, process.Name, 5)
	usChecker.Start(&wg)
	usChecker.Register <- mqListener.CanExit

	// Download files that should be used as configuration files first
	configDownloader := config.NewDownloader(env.SidecarConfig)
	configDownloader.Start(&wg)

	// Track lineage information by posting it on a message queue
	lineageTracker := lineage.NewMqTracker(process.Name, process.PipelineHash, brokerURL, mqLineageBridge)
	lineageTracker.Start(&wg)

	wg.Wait()
	log.Infof("Ran for %v", time.Now().Sub(startTime))
}
