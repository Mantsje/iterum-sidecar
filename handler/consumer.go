package handler

import (
	"net"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/socket"
	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/sidecar/garbage"

	"github.com/prometheus/common/log"
)

// ProcessedFileHandler is a handler for a socket that receives processed files from the transformation step
func ProcessedFileHandler(acknowledger chan transmit.Serializable, fragCollector garbage.FragmentCollector) func(socket socket.Socket, conn net.Conn) {
	return func(socket socket.Socket, conn net.Conn) {
		defer conn.Close()
		for {
			encMsg, err := transmit.ReadMessage(conn)
			if err != nil {
				log.Warnf("Failed to read, closing connection from due to '%v'", err)
				return
			}

			fragMsg := fragmentDesc{}
			errFrag := fragMsg.Deserialize(encMsg)
			doneMsg := desc.FinishedFragmentMessage{}
			errDone := doneMsg.Deserialize(encMsg)
			killMsg := desc.KillMessage{}
			errKill := killMsg.Deserialize(encMsg)

			if errFrag == nil {
				// Default behaviour
				// unwrap socket fragmentDesc into general type before posting on output
				lfd := fragMsg.LocalFragmentDesc
				if len(lfd.Metadata.Predecessors) < 1 {
					log.Errorf("Returned local fragment did not contain any predecessors")
					continue
				}
				socket.Channel <- &lfd
			} else if errDone == nil {
				acknowledger <- &doneMsg
				fragCollector.Collect <- doneMsg.FragmentID
			} else if errKill == nil {
				log.Info("Received kill message, stopping consumer...")
				defer close(socket.Channel)
				defer close(acknowledger)
				defer socket.Stop()
				return
			} else {
				// Error handling
				switch errFrag.(type) {
				case *transmit.SerializationError:
					log.Errorf("Could not decode '%v' due to one of:", string(encMsg))
					log.Errorln(errFrag)
					log.Errorln(errDone)
					log.Fatalln(errKill)
				default:
					log.Errorf("'%v', closing connection", errFrag)
					return
				}
			}

		}
	}
}
