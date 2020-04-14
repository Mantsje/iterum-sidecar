package messageq

import (
	"fmt"
	"os"

	"github.com/prometheus/common/log"

	"github.com/Mantsje/iterum-sidecar/data"
	"github.com/Mantsje/iterum-sidecar/util"
	"github.com/streadway/amqp"
)

// Listener listens on the rabbitMQ messagequeue and redirects messages on the INPUT_QUEUE to a channel
func Listener(channel chan<- data.RemoteFragmentDesc) {
	fmt.Printf("Started goroutine which should listen to the MQ.\n")
	fmt.Printf("Connecting to %s.\n", os.Getenv("BROKER_URL"))
	conn, err := amqp.Dial(os.Getenv("BROKER_URL"))
	util.Ensure(err, "Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.Ensure(err, "Opened channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		os.Getenv("INPUT_QUEUE"), // name
		false,                    // durable
		false,                    // delete when unused
		false,                    // exclusive
		false,                    // no-wait
		nil,                      // arguments
	)
	util.Ensure(err, "Created queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	util.Ensure(err, "Registered consumer")

	fmt.Printf("Started listening for messages from the MQ.\n")
	for message := range msgs {
		var mqFragment MqFragmentDesc
		err := mqFragment.Deserialize(message.Body)
		if err != nil {
			log.Errorln(err)
		}
		fmt.Printf("Received a mqFragment: %s\n", mqFragment)
		var remoteFragment = mqFragment.RemoteFragmentDesc
		fmt.Printf("Unwrapping to remoteFragment: %s\n", remoteFragment)

		channel <- remoteFragment
	}
	fmt.Printf("Processed all messages...\n")

}
