package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime"

	"github.com/Mantsje/iterum-sidecar/util"
	"github.com/prometheus/common/log"
	"github.com/streadway/amqp"
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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	targetSocket := os.Getenv("DATA_VOLUME_PATH") + "/go.sock"
	socket, err := NewSocket(targetSocket, 10, sendReadiedFiles)
	util.Ensure(err, "Socket succesfully opened and listening")
	socket.Start()

	go func() {
		fmt.Printf("Started goroutine which should listen to the message queue.\n")
		fmt.Printf("Connecting to %s.\n", os.Getenv("BROKER_URL"))
		conn, err := amqp.Dial(os.Getenv("BROKER_URL"))
		failOnError(err, "Failed to connect to RabbitMQ")
		defer conn.Close()

		ch, err := conn.Channel()
		failOnError(err, "Failed to open a channel")
		defer ch.Close()

		q, err := ch.QueueDeclare(
			os.Getenv("QUEUE"), // name
			false,              // durable
			false,              // delete when unused
			false,              // exclusive
			false,              // no-wait
			nil,                // arguments
		)
		failOnError(err, "Failed to declare a queue")

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")

		for message := range msgs {
			fragment := fmt.Sprintf("%s", message.Body)
			fmt.Printf("Received a message: %s\n", fragment)
			fmt.Printf("putting '%v' on channel\n", fragment)
			socket.Input <- Msg{fragment}
		}
		fmt.Printf("Processed all messages...\n")

	}()

	runtime.Goexit()
}
