package goredis

import (
	"context"

	"github.com/adjust/rmq/v3"
	"github.com/caarlos0/env"
	"github.com/go-redis/redis/v7"
)

const queueTag = "goreport"

// redis configuration
type redisConfig struct {
	Addr string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	Pass string `env:"REDIS_PASS" envDefault:""`
	DB   int    `env:"REDIS_DB" evnDefault:"10"`
}

// RedisWrapper Redis wrapper to handle pub/sub calls
type RedisWrapper interface {
	Ping(ctx context.Context) (string, error)
	// Publish into Pub/Sub
	Publish(ctx context.Context, channel string, message []byte) error
	// Subscribe into Pub/Sub
	Subscribe(ctx context.Context, channel string, notifyTo chan string)
	// CreateQueue creates a messaging queue
	CreateQueue(queueName string, errChan chan error) (RedisQueueWrapper, error)
	// CreateStream creates a stream that sends incoming messages into a buffered channel
	CreateStream(streamName string, bufferSize int) RedisStreamWrapper
}

type redisWapper struct {
	C *redis.Client
}

// InitClient get a redis wrapper instance
func InitClient() (RedisWrapper, error) {
	config := redisConfig{}
	if err := env.Parse(&config); err != nil {
		return nil, err
	}
	c := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Pass,
		DB:       config.DB,
	})

	return &redisWapper{
		C: c,
	}, nil
}

// Ping ping redis server
func (w *redisWapper) Ping(ctx context.Context) (string, error) {
	return w.C.Ping().Result()
}

// Publish publish message into a channel
func (w *redisWapper) Publish(ctx context.Context, channel string, message []byte) error {
	return w.C.Publish(channel, message).Err()
}

// Subscribe subscrite to listen messages from a channel
func (w *redisWapper) Subscribe(ctx context.Context, channel string, notifyTo chan string) {
	sub := w.C.Subscribe(channel)
	ch := sub.Channel()
	for msg := range ch {
		notifyTo <- msg.Payload
	}
}

// CreateQueue creates a messaging queue instance
func (w *redisWapper) CreateQueue(queueName string, errChan chan error) (RedisQueueWrapper, error) {
	connection, err := rmq.OpenConnectionWithRedisClient(queueTag, w.C, errChan)
	if err != nil {
		return nil, err
	}
	queue, err := connection.OpenQueue(queueName)
	if err != nil {
		return nil, err
	}

	return &redisQueueWrapper{
		queue: queue,
	}, nil
}

// CreateStream creates a stream that sends incoming messages into a buffered channel
func (w *redisWapper) CreateStream(streamName string, bufferSize int) RedisStreamWrapper {
	return &redisStreamWrapper{
		c:           w.C,
		stream:      streamName,
		bufferSize:  bufferSize,
		messageChan: make(chan interface{}, bufferSize),
	}
}
