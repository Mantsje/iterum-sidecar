package messageq

import (
	"fmt"
	"os"

	"github.com/prometheus/common/log"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/util"
	"github.com/streadway/amqp"
)

// Listener is the structure that listens to RabbitMQ and redirects messages to a channel
type Listener struct {
	MqOutput chan<- data.RemoteFragmentDesc
}

// NewListener creates a new message queue listener
func NewListener(channel chan<- data.RemoteFragmentDesc) (listener Listener, err error) {

	listener = Listener{
		channel,
	}
	return
}

// StartBlocking listens on the rabbitMQ messagequeue and redirects messages on the INPUT_QUEUE to a channel
func (listener Listener) StartBlocking() {

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

	mqMessages, err := ch.Consume(
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
	for message := range mqMessages {
		var mqFragment MqFragmentDesc
		err := mqFragment.Deserialize(message.Body)
		if err != nil {
			log.Errorln(err)
		}
		fmt.Printf("Received a mqFragment: %s\n", mqFragment)
		var remoteFragment = mqFragment.RemoteFragmentDesc
		fmt.Printf("Unwrapping to remoteFragment: %s\n", remoteFragment)

		listener.MqOutput <- remoteFragment
	}
	fmt.Printf("Processed all messages...\n")

}

// Start asychronously calls StartBlocking via Gorouting
func (listener Listener) Start() {
	go listener.StartBlocking()
}
