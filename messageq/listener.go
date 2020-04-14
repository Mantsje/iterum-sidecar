package messageq

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/streadway/amqp"
)

// Listener listens on the rabbitMQ messagequeue and redirects messages on the INPUT_QUEUE to a channel
func Listener(channel chan<- data.RemoteFragmentDesc) {
	fmt.Printf("Started goroutine which should listen to the MQ.\n")
	fmt.Printf("Connecting to %s.\n", os.Getenv("BROKER_URL"))
	conn, err := amqp.Dial(os.Getenv("BROKER_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		os.Getenv("INPUT_QUEUE"), // name
		false,                    // durable
		false,                    // delete when unused
		false,                    // exclusive
		false,                    // no-wait
		nil,                      // arguments
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

	fmt.Printf("Started listening for messages from the MQ.\n")
	for message := range msgs {
		var mqFragment mqFragmentDesc
		err := json.Unmarshal(message.Body, &mqFragment)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received a mqFragment: %s\n", mqFragment)
		var remoteFragment = mqFragment.RemoteFragmentDesc
		fmt.Printf("Unwrapping to remoteFragment: %s\n", remoteFragment)

		channel <- remoteFragment
	}
	fmt.Printf("Processed all messages...\n")

}
