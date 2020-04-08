package main

import (
	"fmt"

	"github.com/Mantsje/iterum-sidecar/transmit"
)

func consumer(channel chan transmit.Serializable) {
	for {
		msg, ok := <-channel
		if !ok {
			return
		}
		fragmentDesc := msg.(*FragmentDesc)
		fmt.Println(*fragmentDesc)
	}
}
