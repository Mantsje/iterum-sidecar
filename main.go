package main

import (
	"os"
	"runtime"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/store"

	"github.com/iterum-provenance/sidecar/messageq"
	"github.com/iterum-provenance/sidecar/socket"
	"github.com/iterum-provenance/sidecar/util"
)

func main() {
	mqDownloaderBridgeBufferSize := 10
	mqDownloaderBridge := make(chan data.RemoteFragmentDesc, mqDownloaderBridgeBufferSize)

	downloaderSocketBridgeBufferSize := 10
	downloaderSocketBridge := make(chan data.LocalFragmentDesc, downloaderSocketBridgeBufferSize)

	socketUploaderBridgeBufferSize := 10
	socketUploaderBridge := make(chan data.LocalFragmentDesc, socketUploaderBridgeBufferSize)

	uploaderMqBridgeBufferSize := 10
	uploaderMqBridge := make(chan data.RemoteFragmentDesc, uploaderMqBridgeBufferSize)

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
