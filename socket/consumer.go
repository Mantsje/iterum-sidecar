package socket

import (
	"fmt"
	"net"

	"github.com/Mantsje/iterum-sidecar/transmit"
	"github.com/prometheus/common/log"
)

// ProcessedFileHandler is a handler for a socket that receives processed files from the transformation step
func ProcessedFileHandler(socket Socket, conn net.Conn) {
	defer conn.Close()
	for {
		msg := fragmentDesc{}
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

// Consumer is a dummy setup to help test socket
func Consumer(channel chan transmit.Serializable) {
	for {
		msg, ok := <-channel
		if !ok {
			return
		}
		fragDesc := msg.(*fragmentDesc)
		fmt.Printf("Received: '%v'\n", *fragDesc)
	}
}
