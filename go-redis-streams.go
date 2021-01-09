package goredis

import "github.com/go-redis/redis/v7"

// RedisStreamWrapper interface to handle streams
type RedisStreamWrapper interface {
}

type redisStreamWrapper struct {
	c          *redis.Client
	stream     string
	bufferSize int
	limit      int
	ch         chan interface{}
}

// Publish publish data into the stream
func (s redisStreamWrapper) Publish(message interface{}) (string, error) {
	args := redis.XAddArgs{
		Stream: s.stream,
		Values: map[string]interface{}{
			"data": message,
		},
	}
	return s.c.XAdd(&args).Result()
}
