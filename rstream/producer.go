package rstream

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

// Producer represents a message producer
type Producer struct {
	client *redis.Client
	queue  *Queue
}

// NewProducer creates a new Producer
func NewProducer(client *redis.Client, queue *Queue) *Producer {
	return &Producer{
		client: client,
		queue:  queue,
	}
}

// Publish publishes a message to the stream
func (p *Producer) Publish(ctx context.Context, message Message) error {
	streamKey := buildStreamKey(p.queue.Name)

	_, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: message.Map(),
	}).Result()

	if err != nil {
		log.Error().Err(err).Msgf("Failed to publish message to stream %s, message: %s", streamKey, message.ID)
		return err
	}

	log.Debug().Msgf("Published message to stream %s, message: %s", streamKey, message.ID)

	return nil
}
