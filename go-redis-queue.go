package goredis

import (
	"time"

	"github.com/adjust/rmq/v3"
)

const consumerPrefix = "consumer"

// RedisQueueWrapper interface to publish messages into a messaging queue and create consumers
type RedisQueueWrapper interface {
	PublishString(message string) error
	PublishBytes(payload []byte) error
	CreateConsumer(notifyTo chan []byte, errorChan chan error) (RedisQueueConsumer, error)
}

type redisQueueWrapper struct {
	queue rmq.Queue
}

// RedisQueueConsumer interface to consume messages from a messaging queue
type RedisQueueConsumer interface {
	Consume(delivery rmq.Delivery)
}

type redisQueueConsumer struct {
	notifyTo  chan []byte
	errorChan chan error
}

// PublishString publish a string into the messaging queue
func (mq *redisQueueWrapper) PublishString(message string) error {
	return mq.queue.Publish(message)
}

// PublishBytes publish a bytes into the messaging queue
func (mq *redisQueueWrapper) PublishBytes(payload []byte) error {
	return mq.queue.PublishBytes(payload)
}

// CreateConsumer creates a consumer that get messages or errors from the messaging queue
func (mq *redisQueueWrapper) CreateConsumer(notifyTo chan []byte, errorChan chan error) (RedisQueueConsumer, error) {
	prefetchLimit := 10
	pollDuration := time.Second
	err := mq.queue.StartConsuming(int64(prefetchLimit), pollDuration)
	if err != nil {
		return nil, err
	}
	consumer := &redisQueueConsumer{
		notifyTo:  notifyTo,
		errorChan: errorChan,
	}
	_, err = mq.queue.AddConsumer(consumerPrefix, consumer)
	return nil, nil
}

// Consume method that gets a payload from the messaging queue and sends a notification to the corresponding channel
func (c *redisQueueConsumer) Consume(delivery rmq.Delivery) {
	payload := delivery.Payload()
	c.notifyTo <- []byte(payload)
	delivery.Ack() // TODO: Reject if needed
}
