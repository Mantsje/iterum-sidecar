package socket

import (
	"github.com/Mantsje/iterum-sidecar/transmit"
	"github.com/Mantsje/iterum-sidecar/util"
)

// Pipe represents a bidirectional connection between an iterum sidecar and transformation step
// ToTarget and FromTarget refer to the channels in the two sockets
// Messages supposed to go towards Target can be put on ToTarget and message from the Target are put on FromTarget
type Pipe struct {
	SocketTo   Socket
	SocketFrom Socket
	ToTarget   chan transmit.Serializable
	FromTarget chan transmit.Serializable
}

// NewPipe creates and initiates a new Pipe
func NewPipe(fromFile, toFile string, input, output chan transmit.Serializable, fromHandler, toHandler ConnHandler) Pipe {
	toSocket, err := NewSocket(toFile, input, toHandler)
	util.Ensure(err, "Towards Socket succesfully opened and listening")
	fromSocket, err := NewSocket(fromFile, output, fromHandler)
	util.Ensure(err, "From Socket succesfully opened and listening")

	return Pipe{toSocket, fromSocket, toSocket.Channel, fromSocket.Channel}
}

// Start calls start on both of the pipe's sockets
func (p Pipe) Start() {
	p.SocketTo.Start()
	p.SocketFrom.Start()
}
