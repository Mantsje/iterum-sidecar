package socket

import (
	"net"
	"os"

	"github.com/iterum-provenance/sidecar/transmit"
	"github.com/iterum-provenance/sidecar/util"
	"github.com/prometheus/common/log"
)

// Socket is a structure holding a listener, accepting connections
// Channel is a channel that external things can post messages on take from that are
// supposed to be sent to or from the connections
type Socket struct {
	Listener net.Listener
	Channel  chan transmit.Serializable
	handler  ConnHandler
}

// ConnHandler is a handler function ran in a goroutine upon a socket accepting a new connection
type ConnHandler func(Socket, net.Conn)

// NewSocket sets up a listener at the given socketPath and links the passed channel
// with the given bufferSize. It returns an error on failure
func NewSocket(socketPath string, channel chan transmit.Serializable, handler ConnHandler) (s Socket, err error) {
	defer util.ReturnErrOnPanic(&err)
	if _, errExist := os.Stat(socketPath); !os.IsNotExist(errExist) {
		err = os.Remove(socketPath)
		util.Ensure(err, "Existing socket file removed")
	}

	listener, err := net.Listen("unix", socketPath)
	util.Ensure(err, "Server created")

	s = Socket{
		listener,
		channel,
		handler,
	}
	return
}

// StartBlocking enters an endless loop accepting connections and calling the handler function
// in a goroutine
func (s Socket) StartBlocking() {
	for {
		conn, err := s.Listener.Accept()
		defer conn.Close()
		if err != nil {
			log.Warnln("Connection failed")
			return
		}
		go s.handler(s, conn)
	}
}

// Start asychronously calls StartBlocking via Gorouting
func (s Socket) Start() {
	go s.StartBlocking()
}
