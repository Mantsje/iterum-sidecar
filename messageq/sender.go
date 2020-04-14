package messageq

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/prometheus/common/log"
	"github.com/streadway/amqp"
)

// Sender listens to the channel, and send remoteFragments to the message queue on the OUTPUT_QUEUE queue.
func Sender(channel <-chan data.RemoteFragmentDesc) {
	fmt.Printf("Started goroutine which should send to the MQ.\n")
	fmt.Printf("Connecting to %s.\n", os.Getenv("BROKER_URL"))
	conn, err := amqp.Dial(os.Getenv("BROKER_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		os.Getenv("OUTPUT_QUEUE"), // name
		false,                     // durable
		false,                     // delete when unused
		false,                     // exclusive
		false,                     // no-wait
		nil,                       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for remoteFragment := range channel {

		fmt.Printf("Received a remoteFragment to send to the queue: %s\n", remoteFragment)
		mqFragment := newFragmentDesc(remoteFragment)

		body, err := json.Marshal(mqFragment)
		if err != nil {
			log.Errorln(err)
		}

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         body,
			})
	}
	fmt.Printf("Processed all messages...\n")

}
