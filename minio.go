package main

import (
	"fmt"
	"time"

	"github.com/Mantsje/iterum-sidecar/transmit"
)

func retrieveFiles(inputFragments <-chan transmit.Serializable, output chan<- transmit.Serializable) {
	fmt.Printf("Started goroutine which should retrieve files from Minio.\n")

	for fragment := range inputFragments {
		filename := fmt.Sprintf("%s", fragment)
		fmt.Printf("Started downloading file.. %s\n", filename)
		time.Sleep(1 * time.Second)
		fmt.Printf("Downloaded file.. %s\n", filename)

		fmt.Printf("putting '%s' on channel\n", filename)
		output <- &FragmentDesc{filename}
	}
	fmt.Printf("Processed all messages...\n")
}

func storeFiles(outputFragments <-chan transmit.Serializable, messageQueueOutput chan<- transmit.Serializable) {
	fmt.Printf("Started goroutine which should store files from Minio.\n")

	for fragment := range outputFragments {
		filename := fmt.Sprintf("%s", fragment)
		fmt.Printf("Started uploading file.. %s\n", filename)
		time.Sleep(1 * time.Second)
		fmt.Printf("Uploaded file.. %s\n", filename)

		fmt.Printf("Sending file '%s' on channel to be send to MQ\n", filename)
		messageQueueOutput <- &FragmentDesc{filename}
	}
	fmt.Printf("Processed all messages...\n")
}
