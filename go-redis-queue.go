package goredis

import (
	"time"

	"github.com/adjust/rmq/v3"
)

const consumerPrefix = "consumer"

// RedisQueueWrapper interface to publish messages into a messaging queue and create consumers
type RedisQueueWrapper interface {
	// Publish publish to a messaging queue
	Publish(payload []byte) error
	// Subscribe subscribe to a messaging queue
	Subscribe(notifyTo chan []byte, errorChan chan error) error
}

type redisQueueWrapper struct {
	queue rmq.Queue
}

// RedisQueueConsumer interface to consume messages from a messaging queue
type RedisQueueConsumer interface {
	// Consume this method is called automatically when a new message arrive to the messaging queue
	Consume(delivery rmq.Delivery)
}

type redisQueueConsumer struct {
	notifyTo  chan []byte
	errorChan chan error
}

// PublishBytes publish a bytes into the messaging queue
func (mq *redisQueueWrapper) Publish(payload []byte) error {
	return mq.queue.PublishBytes(payload)
}

// CreateConsumer creates a consumer that get messages or errors from the messaging queue
func (mq *redisQueueWrapper) Subscribe(notifyTo chan []byte, errorChan chan error) error {
	prefetchLimit := 10
	pollDuration := time.Second
	err := mq.queue.StartConsuming(int64(prefetchLimit), pollDuration)
	if err != nil {
		return err
	}
	consumer := &redisQueueConsumer{
		notifyTo:  notifyTo,
		errorChan: errorChan,
	}
	_, err = mq.queue.AddConsumer(consumerPrefix, consumer)
	return nil
}

// Consume this method is called automatically when a new message arrive to the messaging queue
// It sends the received message to the notifyTo chan
func (c *redisQueueConsumer) Consume(delivery rmq.Delivery) {
	payload := delivery.Payload()
	c.notifyTo <- []byte(payload)
	delivery.Ack() // TODO: Reject if needed
}
