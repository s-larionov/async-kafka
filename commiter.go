package async_kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
	"time"
)

type Committer struct {
	isRunning bool
	stopped   chan bool
	consumer  *kafka.Consumer
	topic     string
	sync      sync.WaitGroup
	commitsMu sync.Mutex
	commits   map[int32]kafka.Offset
}

func newCommitter(consumer *kafka.Consumer, topic string) *Committer {
	committer := &Committer{
		consumer: consumer,
		topic:    topic,
		commits:  make(map[int32]kafka.Offset),
	}

	return committer
}

func (c *Committer) WaitCommits() {
	c.sync.Wait()
}

func (c *Committer) Start() {
	c.isRunning = true
	c.stopped = make(chan bool, 1)
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <- ticker.C:
				_ = c.commit()
			case <- c.stopped:
				c.WaitCommits()
				return
			}
		}
	}()
}

func (c *Committer) Commit(msg *kafka.Message) {
	c.commitsMu.Lock()
	defer c.commitsMu.Unlock()

	offset, present := c.commits[msg.TopicPartition.Partition]

	if !present {
		c.sync.Add(1)
	}

	if offset > 0 && offset >= msg.TopicPartition.Offset {
		return
	}

	c.commits[msg.TopicPartition.Partition] = msg.TopicPartition.Offset
}

func (c *Committer) commit() error {
	partitions := make([]kafka.TopicPartition, 0)

	c.commitsMu.Lock()
	for partition, offset := range c.commits {
		partitions = append(partitions, kafka.TopicPartition{
			Topic:     &c.topic,
			Partition: partition,
			Offset:    offset + 1, // see implementation of Consumer.CommitMessage
		})
		delete(c.commits, partition)
	}
	c.commitsMu.Unlock()

	_, err := c.consumer.CommitOffsets(partitions)

	// you can see WaitGroup.Done implementation for understanding
	c.sync.Add(-len(partitions))

	return err
}

func (c *Committer) Stop() {
	if c.isRunning {
		c.stopped <- true
	}

	c.isRunning = false
}
