package main

import (
	"net"
	"os"
	"runtime"

	"github.com/prometheus/common/log"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/transmit"
)

func sendReadiedFiles(socket Socket, conn net.Conn) {
	defer conn.Close()
	for {
		// Wait for the next job to come off the queue.
		msg := <-socket.Channel

		// Send the msg over the connection
		err := transmit.EncodeSend(conn, msg)

		// Error handling
		switch err.(type) {
		case *transmit.SerializationError:
			log.Warnf("Could not encode message due to '%v', skipping message", err)
			continue
		case *transmit.ConnectionError:
			log.Warnf("Closing connection towards due to '%v'", err)
			return
		default:
			log.Errorf("%v, closing connection", err)
			return
		case nil:
		}
	}
}

func receiveProcessedFiles(socket Socket, conn net.Conn) {
	defer conn.Close()
	for {
		msg := data.LocalFragmentDesc{}
		err := transmit.DecodeRead(conn, &msg)

		// Error handling
		switch err.(type) {
		case *transmit.SerializationError:
			log.Warnf("Could not encode message due to '%v', skipping message", err)
			continue
		case *transmit.ConnectionError:
			log.Warnf("Closing connection towards due to '%v'", err)
			return
		default:
			log.Errorf("%v, closing connection", err)
			return
		case nil:
		}

		socket.Channel <- &msg
	}
}

func main() {
	toSocketFile := os.Getenv("PWD") + "/build/A_tts.sock"
	fromSocketFile := os.Getenv("PWD") + "/build/A_fts.sock"

	pipe := NewPipe(fromSocketFile, toSocketFile, 10, 10, receiveProcessedFiles, sendReadiedFiles)
	pipe.Start()

	go producer(pipe.ToTarget)
	go consumer(pipe.FromTarget)

	runtime.Goexit()
}
