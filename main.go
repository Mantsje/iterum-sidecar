package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/Mantsje/iterum-sidecar/util"
	"github.com/prometheus/common/log"
)

func sendReadiedFiles(socket Socket, conn net.Conn) {
	for {
		// Wait for the next job to come off the queue.
		msg := <-socket.Input

		// Send the msg to the conn
		data, err := json.Marshal(msg)
		if err != nil {
			log.Errorf("Error: could not marshal Msg due to '%v'", err)
			continue
		}

		fragmentSize := make([]byte, 4)
		binary.LittleEndian.PutUint32(fragmentSize, uint32(len(data)))
		_, err = conn.Write(append(fragmentSize, data...))
		if err != nil {
			log.Warn(err)
			fmt.Println("Closing connection")
			socket.Input <- msg
			return
		}
	}
}

func main() {
	targetSocket := os.Getenv("PWD") + "/build/go.sock"
	socket, err := NewSocket(targetSocket, 10, sendReadiedFiles)
	util.Ensure(err, "Socket succesfully opened and listening")
	socket.Start()

	// Message publisher
	go func() {
		fileIdx := 0
		for {
			time.Sleep(1 * time.Second)
			fragment := fmt.Sprintf("file%d.txt", fileIdx)
			fmt.Printf("putting '%v' on channel\n", fragment)
			socket.Input <- Msg{fragment}
			fileIdx++
		}
	}()

	runtime.Goexit()
}
