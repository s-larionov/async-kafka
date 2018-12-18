package async_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumeCallback func(msg *kafka.Message) error

type Consumer struct {
	isRunning bool
	stopped   chan bool
	consumer  *kafka.Consumer
	committer *Committer
}

func NewConsumer(brokers string, groupId string, topic string) (*Consumer, error) {
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
	}, nil
}

func (c *Consumer) Consume(cb ConsumeCallback) error {
	c.isRunning = true
	c.committer.Start()

	for c.isRunning == true {
		select {
		case _ = <-c.stopped:
			c.committer.WaitCommits()
			c.committer.Stop()
			c.isRunning = false
		default:
			err := c.consume(cb)
			if err != nil {
				c.Stop()
				return err
			}
		}
	}

	return nil
}

func (c *Consumer) consume(cb ConsumeCallback) error {
	event := c.consumer.Poll(1000)

	switch msg := event.(type) {
	case *kafka.Message:
		err := cb(msg)
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

func (c *Consumer) WaitCommits() {
	c.committer.WaitCommits()
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
