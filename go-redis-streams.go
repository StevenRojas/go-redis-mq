package goredis

import (
	"github.com/go-redis/redis/v7"
	"github.com/vmihailenco/msgpack/v5"
)

// RedisStreamWrapper interface to handle streams
type RedisStreamWrapper interface {
	// SetChannels set the message and error channels
	SetChannels(messageChan chan interface{}, errChan chan error)
	// Publish publish data into the stream
	Publish(message interface{}) (string, error)
	// Consume consume messages from the stream with a count limit. If 0 it will consume all messages
	Consume(count int64)
}

type redisStreamWrapper struct {
	c           *redis.Client
	stream      string
	bufferSize  int
	messageChan chan interface{} // Channel where the consumed messages are send
	errChan     chan error
}

// SetChannels set the message and error channels
func (s *redisStreamWrapper) SetChannels(messageChan chan interface{}, errChan chan error) {
	s.messageChan = messageChan
	s.errChan = errChan
}

// Publish publish data into the stream
func (s *redisStreamWrapper) Publish(message interface{}) (string, error) {
	args := redis.XAddArgs{
		Stream: s.stream,
		Values: map[string]interface{}{
			"data": message,
		},
	}
	return s.c.XAdd(&args).Result()
}

// Consume consume messages from the stream with a count limit. If 0 it will consume all messages
func (s *redisStreamWrapper) Consume(count int64) {
	s.messageChan = make(chan interface{}, s.bufferSize)
	go func() {
		for {
			var err error
			var data []redis.XMessage
			if count > 0 {
				data, err = s.c.XRangeN(s.stream, "-", "+", count).Result()
			} else {
				data, err = s.c.XRange(s.stream, "-", "+").Result()
			}
			if err != nil {
				s.errChan <- err
				continue
			}
			for _, element := range data {
				data := []byte(element.Values["data"].(string)) // Get pack message
				var message map[string]interface{}
				err := msgpack.Unmarshal(data, message)
				if err != nil {
					s.errChan <- err
					continue
				}
				s.messageChan <- message
				s.c.XDel(s.stream, element.ID) // Remove consumed message
			}
		}
	}()
}
