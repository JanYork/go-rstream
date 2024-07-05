package rstream

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type Consumer struct {
	client     *redis.Client
	queue      *Queue
	group      string
	consumer   string
	tag        string
	ctx        map[string]interface{}
	mu         sync.Mutex
	wg         sync.WaitGroup
	processing func(Message, string, map[string]interface{}) error
}

func NewConsumer(client *redis.Client, queue *Queue, group, consumer, tag string, ctx map[string]interface{}, processing func(Message, string, map[string]interface{}) error) *Consumer {
	return &Consumer{
		client:     client,
		queue:      queue,
		group:      group,
		consumer:   consumer,
		tag:        tag,
		ctx:        ctx,
		processing: processing,
	}
}

func (c *Consumer) parseMessage(message redis.XMessage) (Message, string, error) {
	id, ok := message.Values["id"].(string)
	if !ok {
		log.Error().Err(errInvalidMessage).Msgf("Failed to parse message ID: %v", message.Values["id"])
		return Message{}, message.ID, errInvalidMessage
	}

	payloadStr, ok := message.Values["payload"].(string)
	if !ok {
		log.Error().Err(errInvalidMessage).Msgf("Failed to parse message payload: %v", message.Values["payload"])
		return Message{}, message.ID, errInvalidMessage
	}

	createdStr, ok := message.Values["created"].(string)
	if !ok {
		log.Error().Err(errInvalidMessage).Msgf("Failed to parse message created: %v", message.Values["created"])
		return Message{}, message.ID, errInvalidMessage
	}

	created, err := time.Parse(time.RFC3339, createdStr)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to parse message created: %v", createdStr)
		return Message{}, message.ID, err
	}

	return Message{
		ID:      id,
		Payload: []byte(payloadStr),
		Created: created,
	}, message.ID, nil
}

var errInvalidMessage = errors.New("invalid message format")

func (c *Consumer) getActiveWorkers(ctx context.Context) (int64, error) {
	// 使用 XPENDING 命令获取挂起消息信息
	pendingInfo, err := c.client.XPending(ctx, buildStreamKey(c.queue.Name), c.group).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get pending messages in getActiveWorkers function")
		return 0, err
	}

	// 返回总的挂起消息数量
	return pendingInfo.Count, nil
}

func (c *Consumer) fetchMessages(ctx context.Context, count int64) ([]redis.XStream, error) {
	return c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  []string{buildStreamKey(c.queue.Name), ">"},
		Count:    count,
		Block:    c.queue.LongPollingTime,
	}).Result()
}

func (c *Consumer) processMessage(ctx context.Context, msg Message, id string) error {
	if c.processing == nil {
		err := errors.New("processing function is nil")
		log.Error().Err(err).Msg("Failed to process message")
		return err
	}

	// 使用 context.WithTimeout 设置消息处理的超时时间
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), c.queue.BlockTime)
	defer cancel()

	r := make(chan error, 1)

	go func() {
		r <- c.processing(msg, c.tag, c.ctx)
	}()

	select {
	case err := <-r:
		if err != nil {
			log.Error().Err(err).Msgf("Message %s processed with error", msg.ID)
			return c.handleFailure(ctx, msg, id, err)
		}

		_, err = c.client.XAck(ctx, buildStreamKey(c.queue.Name), c.group, id).Result()
		if err != nil {
			log.Error().Err(err).Msgf("Failed to acknowledge message %s", msg.ID)
		}
		log.Error().Err(err).Msgf("Message %s processed successfully", msg.ID)
	case <-ctxWithTimeout.Done():
		log.Error().Err(ctxWithTimeout.Err()).Msgf("Message %s processing timed out", msg.ID)
		return c.handleFailure(ctx, msg, id, ctxWithTimeout.Err())
	}

	return nil
}

func (c *Consumer) handleFailure(ctx context.Context, msg Message, id string, err error) error {
	log.Debug().Err(err).Msgf("Handling failure for message %s", id)

	pendingResult, err := c.getPendingResult(ctx, id)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to get pending result for message %s", id)
		return err
	}

	for _, pending := range pendingResult {
		if pending.ID == id && pending.RetryCount >= c.queue.RetryCount {
			if err := c.sendToDeadLetterStream(ctx, msg, id); err != nil {
				log.Error().Err(err).Msgf("Failed to send message to dead letter stream: %v", err)
				return err
			}
		} else {
			if err := c.retryMessage(ctx, id); err != nil {
				log.Error().Err(err).Msgf("Failed to retry message: %v", err)
				return err
			}
		}
	}

	return nil
}

func (c *Consumer) getPendingResult(ctx context.Context, id string) ([]redis.XPendingExt, error) {
	return c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   buildStreamKey(c.queue.Name),
		Group:    c.group,
		Start:    id,
		End:      id,
		Count:    1,
		Consumer: c.consumer,
	}).Result()
}

func (c *Consumer) sendToDeadLetterStream(ctx context.Context, msg Message, id string) error {
	deadLetterStreamKey := buildStreamKey(c.queue.DeadLetterStream)

	log.Debug().Msgf("Message %s has reached max retry count", id)

	_, err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: deadLetterStreamKey,
		Values: msg.Map(),
	}).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to send message to dead letter stream")
		return err
	}

	log.Info().Msgf("Message %s sent to dead letter stream", id)

	_, err = c.client.XAck(ctx, buildStreamKey(c.queue.Name), c.group, id).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to acknowledge message %s", id)
		return err
	}

	return nil
}

func (c *Consumer) retryMessage(ctx context.Context, id string) error {
	log.Debug().Msgf("Retrying message %s", id)
	claimResult, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   buildStreamKey(c.queue.Name),
		Group:    c.group,
		Consumer: c.consumer,
		MinIdle:  0,
		Messages: []string{id},
	}).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to reassign message: %v", err)
		return err
	}

	if len(claimResult) > 0 {
		log.Debug().Msgf("Message %s reassigned successfully", id)

		for _, msg := range claimResult {
			parsedMsg, msgID, parseErr := c.parseMessage(msg)
			if parseErr != nil {
				log.Error().Err(parseErr).Msgf("Failed to parse reassigned message %s", msgID)
				continue
			}
			go func() {
				processErr := c.processMessage(ctx, parsedMsg, msgID)
				if processErr != nil {
					log.Error().Err(processErr).Msgf("Failed to process reassigned message: %v", processErr)
				}
			}()
		}
	} else {
		log.Debug().Msgf("Message %s reassignment resulted in no messages", id)
	}

	return nil
}

func (c *Consumer) checkActiveWorkers(ctx context.Context) (int64, error) {
	activeWorkers, err := c.getActiveWorkers(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get active workers")
		time.Sleep(1 * time.Second)
		return 0, err
	}

	if c.queue.FIFO {
		if activeWorkers >= 1 {
			time.Sleep(100 * time.Millisecond)
			log.Debug().Msg("FIFO queue has active workers")
			return 0, nil
		}

		return 1, nil
	}

	if activeWorkers >= c.queue.MaxConcurrency {
		// TODO: 指数增长
		time.Sleep(1 * time.Second)
		log.Debug().Msg("Max concurrency reached")
		return 0, nil
	}

	return c.queue.MaxConcurrency - activeWorkers, nil
}

func (c *Consumer) processStream(ctx context.Context, stream redis.XStream) {
	for _, message := range stream.Messages {
		msg, id, err := c.parseMessage(message)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to parse message: %s , and discard it", stream.Stream)
			continue
		}

		c.wg.Add(1)

		go func(msg Message) {
			defer c.wg.Done()

			err := c.processMessage(ctx, msg, id)
			if err != nil {
				log.Error().Err(err).Msgf("Failed to process message: %v", err)
			}
		}(msg)
	}
}

func (c *Consumer) Consume(ctx context.Context) error {
	for {
		remaining, err := c.checkActiveWorkers(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to check active workers")
			time.Sleep(100 * time.Millisecond)
			continue
		}

		messages, err := c.fetchMessages(ctx, remaining)

		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}

		for _, stream := range messages {
			log.Debug().Msgf("Processing stream %s", stream.Stream)
			c.processStream(ctx, stream)
		}
	}
}

// TODO：定期扫描未确认的消息，重试或者发送到死信队列
