package main

import (
	"fmt"
	"github.com/s-larionov/async-kafka"
	"os"
)

func main() {
	p, err := async_kafka.NewProducer("127.0.0.1:9092", "foobar_multithread_test")
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer func(){
		fmt.Print("Closing producer ... ")
		p.Close()
		fmt.Println("done")
	}()

	fmt.Printf("Created Producer %v\n", p)

	go func(){
		for e := range p.Errors() {
			fmt.Printf("Delivery failed: %v\n", e)
		}
	}()

	for i := 1; i <= 1000; i++ {
		err := p.Produce(fmt.Sprintf("Message %d", i))
		if err != nil {
			fmt.Println(err)
		}
	}
}
