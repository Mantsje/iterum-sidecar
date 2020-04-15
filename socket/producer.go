package socket

import (
	"fmt"
	"net"
	"time"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/transmit"
	"github.com/prometheus/common/log"
)

// SendFileHandler is a handler function for a socket that sends files to the transformation step
func SendFileHandler(socket Socket, conn net.Conn) {
	defer conn.Close()
	for {
		// Wait for the next job to come off the queue.
		msg := <-socket.Channel

		// wrap general type into specific socket fragmentDesc before sending
		lfd := fragmentDesc{*msg.(*data.LocalFragmentDesc)}

		// Send the msg over the connection
		err := transmit.EncodeSend(conn, &lfd)

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

// Producer is a dummy setup to help test socket
func Producer(channel chan transmit.Serializable) {
	fileIdx := 0
	for {
		time.Sleep(1 * time.Second)
		dummyName := fmt.Sprintf("file%d.txt", fileIdx)
		dummyFile := data.LocalFileDesc{LocalPath: "./input/bucket/" + dummyName, Name: dummyName}
		dummyFiles := []data.LocalFileDesc{dummyFile}
		dummyFragmentDesc := newFragmentDesc(dummyFiles)
		fmt.Printf("putting fragment on channel:'%v'\n", dummyFragmentDesc)
		channel <- &dummyFragmentDesc

		fileIdx++
	}
}
