package socket

import (
	"fmt"
	"net"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/transmit"

	"github.com/prometheus/common/log"
)

// ProcessedFileHandler is a handler for a socket that receives processed files from the transformation step
func ProcessedFileHandler(socket Socket, conn net.Conn) {
	defer conn.Close()
	for {
		encMsg, err := transmit.ReadMessage(conn)

		if err != nil {
			log.Warnf("Failed to read, closing connection towards due to '%v'", err)
			return
		}

		msg := fragmentDesc{}
		errFrag := msg.Deserialize(encMsg)
		doneMsg := desc.NewKillMessage()
		errDone := doneMsg.Deserialize(encMsg)

		if errFrag == nil {
			// Default behaviour
			// unwrap socket fragmentDesc into general type before posting on output
			lfd := msg.LocalFragmentDesc
			socket.Channel <- &lfd
		} else if errDone == nil {
			log.Info("Received kill message, stopping consumer...")
			defer socket.Stop()
			defer close(socket.Channel)
			return
		} else {
			// Error handling
			switch errFrag.(type) {
			case *transmit.SerializationError:
				log.Fatalf("Could not decode message due to '%v'", errFrag)
				continue
			default:
				log.Errorf("'%v', closing connection", errFrag)
				return
			}
		}

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
