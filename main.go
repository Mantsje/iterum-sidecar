package main

import (
	"net"
	"os"
	"runtime"

	"github.com/prometheus/common/log"

	"github.com/Mantsje/iterum-sidecar/transmit"
	"github.com/Mantsje/iterum-sidecar/util"
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
		msg := FragmentDesc{}
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	toSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/A_tts.sock"
	fromSocketFile := os.Getenv("DATA_VOLUME_PATH") + "/A_fts.sock"
	bufferSizeIn := 10
	bufferSizeOut := 10

	toSocket, err := NewSocket(toSocketFile, bufferSizeIn, sendReadiedFiles)
	util.Ensure(err, "Towards Socket succesfully opened and listening")
	toSocket.Start()

	fromSocket, err := NewSocket(fromSocketFile, bufferSizeOut, receiveProcessedFiles)
	util.Ensure(err, "From Socket succesfully opened and listening")
	fromSocket.Start()

	// Listen to MQ, pull files from Minio, send message to TS to start processing the fragment
	messageQueueInput := make(chan transmit.Serializable)
	messageQueueOutput := make(chan transmit.Serializable)

	go listenToMq(messageQueueInput)
	go retrieveFiles(messageQueueInput, toSocket.Channel)
	go storeFiles(fromSocket.Channel, messageQueueOutput)
	go sendToMq(messageQueueOutput)

	runtime.Goexit()
}
