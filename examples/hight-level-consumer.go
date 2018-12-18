package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/s-larionov/async-kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	consumer, err := async_kafka.NewConsumer("127.0.0.1:9092", "foobar_group", "foobar_topic")
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Starting consumer")

	go func(){
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			consumer.Stop()
		}
	}()

	err = consumer.Consume(func(msg *kafka.Message) error {
		fmt.Printf("Message was consumed on %v: %s\n", msg.TopicPartition, string(msg.Value))

		return nil
	})
	if err != nil {
		fmt.Printf("Failed to consume: %s\n", err)
	}

	fmt.Println("Closing consumer")
	err = consumer.Close()
	if err != nil {
		log.Println(err)
	}
}
