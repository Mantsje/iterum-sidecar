package main

import (
	"fmt"
	"os"

	"github.com/Mantsje/iterum-sidecar/transmit"
	"github.com/streadway/amqp"
)

func listenToMq(channel chan transmit.Serializable) {
	fmt.Printf("Started goroutine which should listen to the MQ.\n")
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

	fmt.Printf("Started listening for messages from the MQ.\n")
	for message := range msgs {
		fragment := fmt.Sprintf("%s", message.Body)
		fmt.Printf("Received a message: %s\n", fragment)
		fmt.Printf("putting '%s' on channel to be downloaded.\n", fragment)
		channel <- &FragmentDesc{fragment}
	}
	fmt.Printf("Processed all messages...\n")

}

func sendToMq(channel chan transmit.Serializable) {
	fmt.Printf("Started goroutine which should send to the MQ.\n")
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

	for message := range channel {
		fragment := fmt.Sprintf("%s", message)

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(fragment),
			})
	}
	fmt.Printf("Processed all messages...\n")

}
