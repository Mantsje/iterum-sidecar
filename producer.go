package main

import (
	"fmt"
	"time"

	"github.com/Mantsje/iterum-sidecar/transmit"
)

func producer(channel chan transmit.Serializable) {
	fileIdx := 0
	for {
		time.Sleep(1 * time.Second)
		fragment := fmt.Sprintf("file%d.txt", fileIdx)
		fmt.Printf("putting '%v' on channel\n", fragment)
		channel <- &FragmentDesc{fragment}
		fileIdx++
	}
}
