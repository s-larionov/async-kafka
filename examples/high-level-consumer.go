package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/s-larionov/async-kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	consumer, err := async_kafka.NewConsumer("127.0.0.1:9092", "foobar_group", "foobar_multithread_test", 12)
	//consumer, err := async_kafka.NewSingleThreadConsumer("127.0.0.1:9092", "foobar_group", "foobar_multithread_test")
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

	errors := consumer.Consume(func(msg *kafka.Message, thread int) error {
		fmt.Printf("[Thread %d] Message was consumed on %v: %s\n", thread, msg.TopicPartition, string(msg.Value))

		time.Sleep(time.Second)

		return nil
	})
	if errors != nil {
		log.Println(errors)
	}

	fmt.Println("Closing consumer")
	errors = consumer.Close()
	if len(errors) > 0 {
		log.Println(errors)
	}
}
