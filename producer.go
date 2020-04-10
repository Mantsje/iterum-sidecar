package main

import (
	"fmt"
	"time"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/transmit"
)

func producer(channel chan transmit.Serializable) {
	fileIdx := 0
	for {
		time.Sleep(1 * time.Second)
		dummyName := fmt.Sprintf("file%d.txt", fileIdx)
		dummyFile := data.LocalFileDesc{LocalPath: "./input/bucket/" + dummyName, Name: dummyName}
		dummyFiles := []data.LocalFileDesc{dummyFile}
		dummyFragmentDesc := &data.LocalFragmentDesc{Files: dummyFiles}
		fmt.Printf("putting fragment on channel:'%v'\n", dummyFragmentDesc)
		channel <- dummyFragmentDesc

		fileIdx++
	}
}
