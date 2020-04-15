package main

import (
	"os"
	"runtime"

	"github.com/iterum-provenance/sidecar/store"
	"github.com/iterum-provenance/sidecar/transmit"

	"github.com/iterum-provenance/sidecar/messageq"
	"github.com/iterum-provenance/sidecar/socket"
	"github.com/iterum-provenance/sidecar/util"
)

func main() {
	mqDownloaderBridgeBufferSize := 10
	mqDownloaderBridge := make(chan transmit.Serializable, mqDownloaderBridgeBufferSize)

	downloaderSocketBridgeBufferSize := 10
	downloaderSocketBridge := make(chan transmit.Serializable, downloaderSocketBridgeBufferSize)

	socketUploaderBridgeBufferSize := 10
	socketUploaderBridge := make(chan transmit.Serializable, socketUploaderBridgeBufferSize)

	uploaderMqBridgeBufferSize := 10
	uploaderMqBridge := make(chan transmit.Serializable, uploaderMqBridgeBufferSize)

	// Socket setup
	toSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/tts.sock"
	fromSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/fts.sock"

	toSocket, err := socket.NewSocket(toSocketFile, downloaderSocketBridge, socket.SendFileHandler)
	util.Ensure(err, "Towards Socket succesfully opened and listening")
	toSocket.Start()

	fromSocket, err := socket.NewSocket(fromSocketFile, socketUploaderBridge, socket.ProcessedFileHandler)
	util.Ensure(err, "From Socket succesfully opened and listening")
	fromSocket.Start()

	// Download manager setup
	downloadManager := store.NewDownloadManager(mqDownloaderBridge, downloaderSocketBridge)
	downloadManager.Start()

	// Upload manager setup
	uploadManager := store.NewUploadManager(socketUploaderBridge, uploaderMqBridge)
	uploadManager.Start()

	// MessageQueue setup
	mqListener, err := messageq.NewListener(mqDownloaderBridge)
	util.Ensure(err, "MessageQueue listener succesfully created and listening")
	mqListener.Start()

	mqSender, err := messageq.NewSender(uploaderMqBridge)
	util.Ensure(err, "MessageQueue sender succesfully created and listening")
	mqSender.Start()

	runtime.Goexit()
}
