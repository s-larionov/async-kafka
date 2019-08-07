package async_kafka

import (
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
)

type Producer struct {
	isRunning bool
	producer  *kafka.Producer
	topic     string
	wg        sync.WaitGroup
	errors    chan error
}

func NewProducer(brokers string, topic string) (*Producer, error) {
	defaultConfig := kafka.ConfigMap{
		"bootstrap.servers": brokers,
	}
	
	return NewProducerWithConfig(defaultConfig, topic)
}

func NewProducerWithConfig(config kafka.ConfigMap, topic string) (*Producer, error) {
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, err
	}
	
	p := &Producer{
		producer:  producer,
		topic:     topic,
		errors:    make(chan error),
		isRunning: true,
	}
	
	go p.handleMessages()
	
	return p, nil
}

func (p *Producer) Close() {
	if !p.isRunning {
		return
	}

	p.isRunning = false
	for p.producer.Len() > 0 {
		p.producer.Flush(500)
	}

	p.producer.Close()
	p.wg.Wait()
	close(p.errors)
}

func (p *Producer) Errors() chan error {
	return p.errors
}

func (p *Producer) Produce(message string) error {
	if !p.isRunning {
		return errors.New("producer was stopped")
	}

	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)
}

func (p *Producer) handleMessages() {
	for e := range p.producer.Events() {
		p.wg.Add(1)
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.errors <- ev.TopicPartition.Error
			}
		}
		p.wg.Done()
	}
}
