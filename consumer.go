package async_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
)

type ConsumeCallback func(msg *kafka.Message, thread int) error

type Consumer struct {
	threads   int
	isRunning bool
	stopped   chan bool
	consumer  *kafka.Consumer
	committer *Committer
}

func NewSingleThreadConsumer(brokers string, groupId string, topic string) (*Consumer, error) {
	return NewConsumer(brokers, groupId, topic, 1)
}

func NewConsumer(brokers string, groupId string, topic string, threads int) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 groupId,
		"auto.offset.reset":        "earliest",
		"auto.commit.enable":       false,
		"enable.auto.offset.store": false,
	})
	if err != nil {
		return nil, err
	}

	committer := newCommitter(consumer, topic)

	err = consumer.SubscribeTopics([]string{topic}, func(c *kafka.Consumer, event kafka.Event) error {
		switch msg := event.(type) {
		case kafka.AssignedPartitions:
			return consumer.Assign(msg.Partitions)
		case kafka.RevokedPartitions:
			return consumer.Unassign()
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer:  consumer,
		committer: committer,
		stopped:   make(chan bool, 1),
		threads:   threads,
	}, nil
}

func (c *Consumer) Consume(cb ConsumeCallback) []error {
	c.isRunning = true
	c.committer.Start()

	var wg sync.WaitGroup

	errors := make([]error, 0)

	for i := 1; i <= c.threads; i ++ {
		wg.Add(1)
		go func(thread int) {
			err := c.runConsuming(cb, thread)
			if err != nil {
				errors = append(errors, err)
				c.stopped <- true
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	if len(errors) == 0 {
		return nil
	}

	return errors
}

func (c *Consumer) runConsuming(cb ConsumeCallback, thread int) error {
	for c.isRunning == true {
		select {
		case _ = <-c.stopped:
			c.isRunning = false
			c.committer.Stop()
		default:
			err := c.consume(cb, thread)
			if err != nil {
				c.Stop()
				return err
			}
		}
	}

	return nil
}

func (c *Consumer) consume(cb ConsumeCallback, thread int) error {
	event := c.consumer.Poll(1000)

	switch msg := event.(type) {
	case *kafka.Message:
		err := cb(msg, thread)
		if err != nil {
			return err
		} else {
			c.committer.Commit(msg)
		}
	case kafka.Error:
		return msg
	case kafka.PartitionEOF:
		// Do nothing
	}

	return nil
}

func (c *Consumer) Close() error {
	if c.isRunning {
		c.Stop()
	}

	return c.consumer.Close()
}

func (c *Consumer) Stop() {
	if c.isRunning {
		c.stopped <- true
	}

	c.isRunning = false
}
