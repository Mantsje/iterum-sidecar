package main

import (
	"net"
	"os"

	"github.com/Mantsje/iterum-sidecar/util"
	"github.com/prometheus/common/log"
)

// Msg is the structure used to send to the transformation step
type Msg struct {
	File string `json:"file"`
}

// Socket is a structure holding a listener, accepting connections
// Input is a channel that external things can post messages on that are
// supposed to be sent to the connections
type Socket struct {
	Listener net.Listener
	Input    chan Msg
	handler  func(Socket, net.Conn)
}

// NewSocket sets up a listener at the given socketPath and creates an input channel
// with the given bufferSize. It returns an error on failure
func NewSocket(socketPath string, inputBufferSize int, handler func(Socket, net.Conn)) (s Socket, err error) {
	defer util.ReturnErrOnPanic(&err)
	if _, errExist := os.Stat(socketPath); !os.IsNotExist(errExist) {
		err = os.Remove(socketPath)
		util.Ensure(err, "Existing socket file removed")
	}

	listener, err := net.Listen("unix", socketPath)
	util.Ensure(err, "Server created")

	input := make(chan Msg, inputBufferSize)

	s = Socket{
		listener,
		input,
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
