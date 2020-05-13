package messageq

import (
	"fmt"
	"sync"

	"github.com/prometheus/common/log"

	desc "github.com/iterum-provenance/iterum-go/descriptors"
	"github.com/iterum-provenance/iterum-go/util"

	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/streadway/amqp"
)

// Sender is the structure that listens to a channel and redirects messages to rabbitMQ
type Sender struct {
	toSend      <-chan transmit.Serializable // data.RemoteFragmentDesc
	TargetQueue string
	BrokerURL   string
	fragments   int
}

// NewSender creates a new sender which receives messages from a channel and sends them on the message queue.
func NewSender(toSend <-chan transmit.Serializable, brokerURL, targetQueue string) (sender Sender, err error) {

	sender = Sender{
		toSend,
		targetQueue,
		brokerURL,
		0,
	}
	return
}

// DeclareQueue defines an amqp Queue
func (sender Sender) DeclareQueue(queueName string, ch *amqp.Channel) amqp.Queue {
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	util.Ensure(err, fmt.Sprintf("Created queue %v", queueName))
	return q
}

// StartBlocking listens to the channel, and send remoteFragments to the message queue on the OUTPUT_QUEUE queue.
func (sender Sender) StartBlocking() {

	log.Infof("Connecting to %s.\n", sender.BrokerURL)
	conn, err := amqp.Dial(sender.BrokerURL)
	util.Ensure(err, "Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.Ensure(err, "Opened channel")
	defer ch.Close()

	queues := make(map[string]amqp.Queue)

	targetq := sender.DeclareQueue(sender.TargetQueue, ch)
	queues[sender.TargetQueue] = targetq

	for {
		remoteFragMsg, ok := <-sender.toSend
		if !ok {
			break
		}
		remoteFragment := *remoteFragMsg.(*desc.RemoteFragmentDesc)

		var q amqp.Queue
		if remoteFragment.Metadata.TargetQueue != nil {
			queueName := *remoteFragment.Metadata.TargetQueue
			if _, ok := queues[queueName]; !ok {
				queues[queueName] = sender.DeclareQueue(queueName, ch)
			}
			q = queues[queueName]
		} else {
			q = targetq
		}

		log.Debugf("Sending %v to queue '%v'\n", remoteFragment, q.Name)
		mqFragment := newFragmentDesc(remoteFragment)

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
		sender.fragments++
	}

	sender.Stop()
}

// Start asychronously calls StartBlocking via Gorouting
func (sender Sender) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		sender.StartBlocking()
	}()
}

// Stop finishes up and notifies the user of its progress
func (sender Sender) Stop() {
	log.Infof("MQSender finishing up, published %v messages\n", sender.fragments)
}
