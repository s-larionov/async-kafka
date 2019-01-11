package async_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
)

type Producer struct {
	producer *kafka.Producer
	topic    string
	wg       sync.WaitGroup
	errors   chan error
}

func NewProducer(brokers string, topic string) (*Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return nil, err
	}

	p := &Producer{
		producer: producer,
		topic:    topic,
		errors:   make(chan error),
	}

	go p.handleMessages()

	return p, nil
}

func (p *Producer) Close() {
	p.wg.Wait()
	p.producer.Close()
	close(p.errors)
}

func (p *Producer) Errors() chan error {
	return p.errors
}

func (p *Producer) Produce(message string) error {
	p.wg.Add(1)

	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)
}

func (p *Producer) handleMessages() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.errors <- ev.TopicPartition.Error
			}
		}

		p.wg.Done()
	}
}
