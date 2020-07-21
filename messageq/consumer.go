package messageq

import (
	"fmt"
	"sync"

	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/iterum-go/util"
	"github.com/prometheus/common/log"
	"github.com/streadway/amqp"
)

// Consumer consumes messages from the messagequeue without acknowledging them
type Consumer struct {
	Output    chan<- transmit.Serializable // *desc.RemoteFragmentDesc
	Unacked   chan amqp.Delivery
	Exit      chan bool
	mqChannel *amqp.Channel
	QueueName string
	consumed  int
}

// NewConsumer creates a message consumer for a listener
func NewConsumer(out chan transmit.Serializable, mqChannel *amqp.Channel, inputQueue string) (consumer Consumer) {
	return Consumer{
		out,
		make(chan amqp.Delivery, 10),
		make(chan bool, 1),
		mqChannel,
		inputQueue,
		0,
	}
}

func (consumer *Consumer) handleRemoteFragment(message amqp.Delivery) {
	var mqFragment MqFragmentDesc
	err := mqFragment.Deserialize(message.Body)
	if err != nil {
		log.Errorln(err)
	}
	log.Debugf("Received a mqFragment: %v\n", mqFragment)
	var remoteFragment = mqFragment.RemoteFragmentDesc

	consumer.Output <- &remoteFragment
	consumer.Unacked <- message
}

// StartBlocking listens on the rabbitMQ messagequeue and redirects messages on the INPUT_QUEUE to a channel
func (consumer *Consumer) StartBlocking() {
	q, err := consumer.mqChannel.QueueDeclare(
		consumer.QueueName, // name
		false,              // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	util.Ensure(err, fmt.Sprintf("Created queue '%v'", consumer.QueueName))

	mqMessages, err := consumer.mqChannel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	util.Ensure(err, "Registered consumer to queue")

	log.Infof("Started consuming messages from the MQ.\n")
consumeloop:
	for {
		select {
		case message, ok := <-mqMessages:
			if !ok {
				log.Errorln("Consuming message from remote MQ went wrong, possibly channel is closed")
				message.Ack(true)
			} else {
				consumer.handleRemoteFragment(message)
				consumer.consumed++
			}
		case <-consumer.Exit:
			break consumeloop
		}
	}
	consumer.Stop()
}

// Start asychronously calls StartBlocking via a Goroutine
func (consumer *Consumer) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.StartBlocking()
	}()
}

// Stop finishes up the consumer
func (consumer *Consumer) Stop() {
	log.Infof("MQConsumer finishing up, consumed %v messages\n", consumer.consumed)
	close(consumer.Output)
	close(consumer.Unacked)
}
