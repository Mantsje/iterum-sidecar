package socket

import (
	"fmt"

	"github.com/Mantsje/iterum-sidecar/transmit"
)

// Consumer is a dummy setup to help test socket
func Consumer(channel chan transmit.Serializable) {
	for {
		msg, ok := <-channel
		if !ok {
			return
		}
		fragDesc := msg.(*fragmentDesc)
		fmt.Println(*fragDesc)
	}
}
