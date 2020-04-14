package main

import (
	"os"
	"runtime"

	"github.com/Mantsje/iterum-sidecar/store"

	"github.com/Mantsje/iterum-sidecar/messageq"
	"github.com/Mantsje/iterum-sidecar/socket"
	"github.com/Mantsje/iterum-sidecar/util"
)

func main() {

	toSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/A_tts.sock"
	fromSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/A_fts.sock"
	bufferSizeIn := 10
	downloaderBufferSize := 10
	bufferSizeOut := 10

	toSocket, err := socket.NewSocket(toSocketFile, bufferSizeIn, socket.SendFileHandler)
	util.Ensure(err, "Towards Socket succesfully opened and listening")
	toSocket.Start()

	fromSocket, err := socket.NewSocket(fromSocketFile, bufferSizeOut, socket.ProcessedFileHandler)
	util.Ensure(err, "From Socket succesfully opened and listening")
	fromSocket.Start()

	downloadManager := store.NewDownloadManager(downloaderBufferSize)

	// Listen to MQ, pull files from Minio, send message to TS to start processing the fragment
	// messageQueueInput := make(chan data.RemoteFragmentDesc)
	// messageQueueOutput := make(chan data.RemoteFragmentDesc)

	go messageq.Listener(downloadManager.ToDownload)
	// go retrieveFiles(messageQueueInput, toSocket.Channel)
	// go storeFiles(fromSocket.Channel, messageQueueOutput)
	// go messageq.Sender(messageQueueOutput)

	runtime.Goexit()
}
