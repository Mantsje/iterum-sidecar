package main

import (
	"sync"

	"github.com/iterum-provenance/sidecar/socket"
	"github.com/iterum-provenance/sidecar/transmit"
	"github.com/iterum-provenance/sidecar/util"
)

func main() {

	var wg sync.WaitGroup
	socketUploaderBridgeBufferSize := 10
	socketUploaderBridge := make(chan transmit.Serializable, socketUploaderBridgeBufferSize)

	// Socket setup
	toSocketFile := "./build/tts.sock"
	toSocket, err := socket.NewSocket(toSocketFile, socketUploaderBridge, socket.SendFileHandler)
	util.Ensure(err, "Towards Socket succesfully opened and listening")
	toSocket.Start(&wg)
	toSocket.Stop()
	wg.Wait()

	// var wg sync.WaitGroup

	// mqDownloaderBridgeBufferSize := 10
	// mqDownloaderBridge := make(chan transmit.Serializable, mqDownloaderBridgeBufferSize)

	// downloaderSocketBridgeBufferSize := 10
	// downloaderSocketBridge := make(chan transmit.Serializable, downloaderSocketBridgeBufferSize)

	// socketUploaderBridgeBufferSize := 10
	// socketUploaderBridge := make(chan transmit.Serializable, socketUploaderBridgeBufferSize)

	// uploaderMqBridgeBufferSize := 10
	// uploaderMqBridge := make(chan transmit.Serializable, uploaderMqBridgeBufferSize)

	// // Socket setup
	// toSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/tts.sock"
	// fromSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/fts.sock"

	// toSocket, err := socket.NewSocket(toSocketFile, downloaderSocketBridge, socket.SendFileHandler)
	// util.Ensure(err, "Towards Socket succesfully opened and listening")
	// toSocket.Start(&wg)

	// fromSocket, err := socket.NewSocket(fromSocketFile, socketUploaderBridge, socket.ProcessedFileHandler)
	// util.Ensure(err, "From Socket succesfully opened and listening")
	// fromSocket.Start(&wg)

	// // Download manager setup
	// downloadManager := store.NewDownloadManager(mqDownloaderBridge, downloaderSocketBridge)
	// downloadManager.Start(&wg)

	// // Upload manager setup
	// uploadManager := store.NewUploadManager(socketUploaderBridge, uploaderMqBridge)
	// uploadManager.Start(&wg)

	// brokerURL := os.Getenv("BROKER_URL")
	// outputQueue := os.Getenv("OUTPUT_QUEUE")
	// inputQueue := os.Getenv("INPUT_QUEUE")

	// // MessageQueue setup
	// mqListener, err := messageq.NewListener(mqDownloaderBridge, brokerURL, inputQueue)
	// util.Ensure(err, "MessageQueue listener succesfully created and listening")
	// mqListener.Start(&wg)

	// mqSender, err := messageq.NewSender(uploaderMqBridge, brokerURL, outputQueue)
	// util.Ensure(err, "MessageQueue sender succesfully created and listening")
	// mqSender.Start(&wg)

	// wg.Wait()
}
