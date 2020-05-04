package messageq

import (
	"sync"
	"time"

	"github.com/prometheus/common/log"
	"github.com/streadway/amqp"

	"github.com/iterum-provenance/iterum-go/transmit"
	"github.com/iterum-provenance/iterum-go/util"
)

// Listener is the structure that listens to RabbitMQ and redirects messages to a channel
type Listener struct {
	MqOutput    chan<- transmit.Serializable // data.RemoteFragmentDesc
	BrokerURL   string
	TargetQueue string
	CanExit     chan bool
	exit        chan bool
	fragments   int
}

// NewListener creates a new message queue listener
func NewListener(channel chan<- transmit.Serializable, brokerURL, inputQueue string) (listener Listener, err error) {

	listener = Listener{
		channel,
		brokerURL,
		inputQueue,
		make(chan bool, 1),
		make(chan bool, 1),
		0,
	}
	return
}

func (listener *Listener) handleRemoteFragment(message amqp.Delivery) {
	var mqFragment MqFragmentDesc
	err := mqFragment.Deserialize(message.Body)
	if err != nil {
		log.Errorln(err)
	}
	log.Debugf("Received a mqFragment: %v\n", mqFragment)
	var remoteFragment = mqFragment.RemoteFragmentDesc

	listener.MqOutput <- &remoteFragment
}

func (listener *Listener) messagesLeftChecker(ch *amqp.Channel) {
	var err error = nil
	qChecker := amqp.Queue{Messages: 9999999} /// >0 init value. form of do-while
	for qChecker.Messages > 0 {
		qChecker, err = ch.QueueInspect(listener.TargetQueue)
		if err != nil {
			log.Fatal(err)
		}
		log.Infof("%v messages remaining in Queue. Closing when zero\n", qChecker.Messages)
		time.Sleep(5 * time.Second)
	}
	log.Infof("MQListener processed all messages\n")
	listener.exit <- true
	close(listener.exit)
}

// StartBlocking listens on the rabbitMQ messagequeue and redirects messages on the INPUT_QUEUE to a channel
func (listener *Listener) StartBlocking() {

	log.Infof("Connecting to %s.\n", listener.BrokerURL)
	conn, err := amqp.Dial(listener.BrokerURL)
	util.Ensure(err, "Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.Ensure(err, "Opened channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		listener.TargetQueue, // name
		false,                // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
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

	log.Infof("Started listening for messages from the MQ.\n")
	for listener.CanExit != nil || mqMessages != nil {
		select {
		case message := <-mqMessages:
			listener.handleRemoteFragment(message)
			listener.fragments++
		case canExitOnEmpty := <-listener.CanExit:
			if canExitOnEmpty {
				listener.CanExit = nil
				go listener.messagesLeftChecker(ch)
			}
		case <-listener.exit:
			listener.exit = nil
			mqMessages = nil
		}
	}
	listener.Stop()
}

// Start asychronously calls StartBlocking via Gorouting
func (listener *Listener) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		listener.StartBlocking()
	}()
}

// Stop finishes up and notifies the user of its progress
func (listener *Listener) Stop() {
	log.Infof("MQListener finishing up, consumed %v messages\n", listener.fragments)
	close(listener.MqOutput)
}
