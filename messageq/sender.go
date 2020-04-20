package messageq

import (
	"fmt"
	"sync"

	"github.com/prometheus/common/log"

	"github.com/iterum-provenance/sidecar/data"
	"github.com/iterum-provenance/sidecar/transmit"
	"github.com/iterum-provenance/sidecar/util"
	"github.com/streadway/amqp"
)

// Sender is the structure that listens to a channel and redirects messages to rabbitMQ
type Sender struct {
	toSend      <-chan transmit.Serializable // data.RemoteFragmentDesc
	TargetQueue string
	BrokerURL   string
}

// NewSender creates a new sender which receives messages from a channel and sends them on the message queue.
func NewSender(toSend <-chan transmit.Serializable, brokerURL, targetQueue string) (sender Sender, err error) {

	sender = Sender{
		toSend,
		targetQueue,
		brokerURL,
	}
	return
}

// StartBlocking listens to the channel, and send remoteFragments to the message queue on the OUTPUT_QUEUE queue.
func (sender Sender) StartBlocking() {

	fmt.Printf("Connecting to %s.\n", sender.BrokerURL)
	conn, err := amqp.Dial(sender.BrokerURL)
	util.Ensure(err, "Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.Ensure(err, "Opened channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		sender.TargetQueue, // name
		false,              // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	util.Ensure(err, "Created queue")

	for remoteFragment := range sender.toSend {

		fmt.Printf("Received a remoteFragment to send to the queue: %s\n", remoteFragment)
		mqFragment := newFragmentDesc(*remoteFragment.(*data.RemoteFragmentDesc))

		body, err := mqFragment.Serialize()
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
		if err != nil {
			log.Errorln(err)
		}
	}
	fmt.Printf("Processed all messages...\n")

}

// Start asychronously calls StartBlocking via Gorouting
func (sender Sender) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		sender.StartBlocking()
	}()
}
