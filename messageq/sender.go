package messageq

import (
	"fmt"
	"os"

	"github.com/prometheus/common/log"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/util"
	"github.com/streadway/amqp"
)

// Sender listens to the channel, and send remoteFragments to the message queue on the OUTPUT_QUEUE queue.
func Sender(channel <-chan data.RemoteFragmentDesc) {
	fmt.Printf("Started goroutine which should send to the MQ.\n")
	fmt.Printf("Connecting to %s.\n", os.Getenv("BROKER_URL"))
	conn, err := amqp.Dial(os.Getenv("BROKER_URL"))
	util.Ensure(err, "Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.Ensure(err, "Opened channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		os.Getenv("OUTPUT_QUEUE"), // name
		false,                     // durable
		false,                     // delete when unused
		false,                     // exclusive
		false,                     // no-wait
		nil,                       // arguments
	)
	util.Ensure(err, "Created queue")

	for remoteFragment := range channel {

		fmt.Printf("Received a remoteFragment to send to the queue: %s\n", remoteFragment)
		mqFragment := newFragmentDesc(remoteFragment)

		body, err := mqFragment.Serialize()
		if err != nil {
			log.Error(err)
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
