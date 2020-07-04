package main

import (
	"sync"

	envcomm "github.com/iterum-provenance/iterum-go/env"
	"github.com/iterum-provenance/iterum-go/minio"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/iterum-provenance/sidecar/env"
	"github.com/iterum-provenance/sidecar/env/config"
	"github.com/iterum-provenance/sidecar/garbage"
	"github.com/iterum-provenance/sidecar/lineage"
	"github.com/iterum-provenance/sidecar/manager"
	"github.com/iterum-provenance/sidecar/messageq"
	"github.com/iterum-provenance/sidecar/socket"
	"github.com/iterum-provenance/sidecar/store"
)

func main() {
	// log.Base().SetLevel("Debug")

	var wg sync.WaitGroup

	mqDownloaderBridgeBufferSize := 10
	mqDownloaderBridge := make(chan transmit.Serializable, mqDownloaderBridgeBufferSize)

	downloaderSocketBridgeBufferSize := 10
	downloaderSocketBridge := make(chan transmit.Serializable, downloaderSocketBridgeBufferSize)

	socketUploaderBridgeBufferSize := 10
	socketUploaderBridge := make(chan transmit.Serializable, socketUploaderBridgeBufferSize)

	uploaderMqBridgeBufferSize := 10
	uploaderMqBridge := make(chan transmit.Serializable, uploaderMqBridgeBufferSize)

	socketAcknowledgerBridgeBufferSize := 10
	socketAcknowledgerBridge := make(chan transmit.Serializable, socketAcknowledgerBridgeBufferSize)

	mqLineageBridgeBufferSize := 10
	mqLineageBridge := make(chan transmit.Serializable, mqLineageBridgeBufferSize)

	fragCollector := garbage.NewFragmentCollector()
	fragCollector.Start(&wg)

	// Socket setup
	toSocketFile := env.TransformationStepInputSocket
	fromSocketFile := env.TransformationStepOutputSocket

	toSocket, err := socket.NewSocket(toSocketFile, downloaderSocketBridge, socket.SendFileHandler(fragCollector))
	util.Ensure(err, "Towards Socket succesfully opened and listening")
	toSocket.Start(&wg)

	fromSocket, err := socket.NewSocket(fromSocketFile, socketUploaderBridge, socket.ProcessedFileHandler(socketAcknowledgerBridge, fragCollector))
	util.Ensure(err, "From Socket succesfully opened and listening")
	fromSocket.Start(&wg)

	// Download manager setup
	minioConfigDown, err := minio.NewMinioConfigFromEnv() // defaults to an output setup
	util.PanicIfErr(err, "")
	minioConfigDown.TargetBucket = "INVALID" // adjust such that the target output is unusable
	err = minioConfigDown.Connect()
	util.PanicIfErr(err, "")
	downloadManager := store.NewDownloadManager(minioConfigDown, mqDownloaderBridge, downloaderSocketBridge)
	downloadManager.Start(&wg)

	configDownloader := config.NewDownloader(env.SidecarConfig, minioConfigDown)
	configDownloader.Start(&wg)

	// Upload manager setup
	// Define and connect to minio storage
	minioConfigUp, err := minio.NewMinioConfigFromEnv() // defaults to an output setup
	util.PanicIfErr(err, "")
	err = minioConfigUp.Connect()
	util.PanicIfErr(err, "")
	uploadManager := store.NewUploadManager(minioConfigUp, socketUploaderBridge, uploaderMqBridge, env.SidecarConfig, fragCollector)
	uploadManager.Start(&wg)

	// MessageQueue setup
	brokerURL := envcomm.MQBrokerURL
	outputQueue := envcomm.MQOutputQueue
	inputQueue := envcomm.MQInputQueue
	prefetchCount := envcomm.MQPrefetchCount

	mqListener, err := messageq.NewListener(mqDownloaderBridge, socketAcknowledgerBridge, brokerURL, inputQueue, prefetchCount)
	util.Ensure(err, "MessageQueue listener succesfully created and listening")
	mqListener.Start(&wg)

	mqSender, err := messageq.NewSender(uploaderMqBridge, mqLineageBridge, brokerURL, outputQueue)
	util.Ensure(err, "MessageQueue sender succesfully created and listening")
	mqSender.Start(&wg)

	usChecker := manager.NewUpstreamChecker(envcomm.ManagerURL, envcomm.PipelineHash, envcomm.ProcessName, 5)
	usChecker.Start(&wg)
	usChecker.Register <- mqListener.CanExit

	lineageTracker := lineage.NewMqTracker(envcomm.ProcessName, envcomm.PipelineHash, brokerURL, mqLineageBridge)
	lineageTracker.Start(&wg)

	wg.Wait()
}
