package main

import (
	"os"
	"runtime"

	"github.com/Mantsje/iterum-sidecar/socket"
)

func main() {
	toSocketFile := os.Getenv("PWD") + "/build/A_tts.sock"
	fromSocketFile := os.Getenv("PWD") + "/build/A_fts.sock"

	pipe := socket.NewPipe(fromSocketFile, toSocketFile, 10, 10, socket.ProcessedFileHandler, socket.SendFileHandler)
	pipe.Start()

	go socket.Producer(pipe.ToTarget)
	go socket.Consumer(pipe.FromTarget)

	runtime.Goexit()
}
