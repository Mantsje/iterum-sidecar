package main

import (
	"fmt"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/transmit"
)

func consumer(channel chan transmit.Serializable) {
	for {
		msg, ok := <-channel
		if !ok {
			return
		}
		fragmentDesc := msg.(*data.LocalFragmentDesc)
		fmt.Println(*fragmentDesc)
	}
}
