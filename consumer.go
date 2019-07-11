package async_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
)

type ConsumeCallback func(msg *kafka.Message, thread int) error

type Consumer struct {
	threads    int
	isRunning  bool
	stopped    chan bool
	consumers  []*kafka.Consumer
	committers []*Committer
}

func NewSingleThreadConsumer(brokers string, groupId string, topic string) (*Consumer, error) {
	return NewConsumer(brokers, groupId, topic, 1)
}

func NewConsumer(brokers string, groupId string, topic string, threads int) (*Consumer, error) {
	return NewConsumerWithRebalanceCb(brokers, groupId, topic, threads, func(consumer *kafka.Consumer, event kafka.Event) error {
		switch msg := event.(type) {
		case kafka.AssignedPartitions:
			return consumer.Assign(msg.Partitions)
		case kafka.RevokedPartitions:
			return consumer.Unassign()
		}

		return nil
	})
}

func NewConsumerWithRebalanceCb(brokers string, groupId string, topic string, threads int, cb kafka.RebalanceCb) (*Consumer, error) {
	consumers := make([]*kafka.Consumer, threads)
	committers := make([]*Committer, threads)
	for i := 0; i < threads; i++ {
		consumer, committer, err := createConsumer(brokers, groupId, topic, cb)

		if err != nil {
			return nil, err
		}

		consumers[i] = consumer
		committers[i] = committer
	}

	return &Consumer{
		consumers:  consumers,
		committers: committers,
		stopped:    make(chan bool, 1),
		threads:    threads,
	}, nil
}

func (c *Consumer) Consume(cb ConsumeCallback) []error {
	c.isRunning = true

	for _, committer := range c.committers {
		committer.Start()
	}

	var wg sync.WaitGroup

	var errorsMu sync.Mutex
	errors := make([]error, 0)

	for i := 0; i < c.threads; i++ {
		wg.Add(1)
		go func(thread int) {
			err := c.runConsuming(cb, thread)
			if err != nil {
				errorsMu.Lock()
				errors = append(errors, err)
				errorsMu.Unlock()
				c.Stop()
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
			c.stopCommitters()
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

func (c *Consumer) stopCommitters() {
	for _, committer := range c.committers {
		committer.Stop()
	}
}

func (c *Consumer) consume(cb ConsumeCallback, thread int) error {
	event := c.consumers[thread].Poll(1000)

	switch msg := event.(type) {
	case *kafka.Message:
		err := cb(msg, thread)
		if err != nil {
			return err
		} else {
			c.committers[thread].Commit(msg)
		}
	case kafka.Error:
		return msg
	case kafka.PartitionEOF:
		// Do nothing
		return nil
	}

	return nil
}

func (c *Consumer) Close() []error {
	c.Stop()

	var errors []error
	for _, consumer := range c.consumers {
		err := consumer.Close()

		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (c *Consumer) Stop() {
	if c.isRunning {
		c.stopped <- true
	}

	c.isRunning = false
}

func createConsumer(brokers string, groupId string, topic string, cb kafka.RebalanceCb) (*kafka.Consumer, *Committer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 groupId,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	})

	if err != nil {
		return nil, nil, err
	}
	committer := newCommitter(consumer, topic)

	err = consumer.Subscribe(topic, cb)
	if err != nil {
		return nil, nil, err
	}

	return consumer, committer, nil
}