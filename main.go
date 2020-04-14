package main

import (
	"os"
	"runtime"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/store"

	"github.com/Mantsje/iterum-sidecar/messageq"
	"github.com/Mantsje/iterum-sidecar/socket"
	"github.com/Mantsje/iterum-sidecar/util"
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

	// MessageQueue setup
	go messageq.Listener(mqDownloaderBridge)
	go messageq.Sender(uploaderMqBridge)

	runtime.Goexit()
}
