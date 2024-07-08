package rstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

type DeadLetterConsumer struct {
	client              *redis.Client
	queue               *Queue
	processing          func(Message) error // 处理消息的函数
	retryCount          int64               // 最大重试次数
	finalFailurePattern int64               // 最终失败模式
	maxConcurrency      int64               // 最大并发数
}

const (
	AbandonPattern = iota
	RetryPattern
)

func NewDeadLetterConsumer(client *redis.Client, queue *Queue, processing func(Message) error, retryCount int64, maxConcurrency int64) *DeadLetterConsumer {
	return &DeadLetterConsumer{
		client:         client,
		queue:          queue,
		processing:     processing,
		retryCount:     retryCount,
		maxConcurrency: maxConcurrency,
	}
}

func (d *DeadLetterConsumer) Consume(ctx context.Context) error {
	sem := make(chan struct{}, d.maxConcurrency)
	var wg sync.WaitGroup

	for {
		if err := d.consumeBatch(ctx, sem, &wg); err != nil {
			return err
		}
	}
}

func (d *DeadLetterConsumer) consumeBatch(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup) error {
	activeWorkers := int64(len(sem))
	toFetch := d.maxConcurrency - activeWorkers

	if toFetch <= 0 {
		time.Sleep(time.Millisecond * 100)
		return nil
	}

	messages, err := d.fetchMessages(ctx, toFetch)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	for _, message := range messages {
		d.processMessageInGoroutine(ctx, sem, wg, message)
	}
	wg.Wait()
	return nil
}

func (d *DeadLetterConsumer) processMessageInGoroutine(ctx context.Context, sem chan struct{}, wg *sync.WaitGroup, msg redis.XMessage) {
	sem <- struct{}{}
	wg.Add(1)
	go func(msg redis.XMessage) {
		defer func() {
			<-sem
			wg.Done()
		}()

		message, streamID, err := d.parseMessage(msg)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to parse message: %s", streamID)
			return
		}

		if err := d.processMessage(ctx, message, streamID); err != nil {
			log.Error().Err(err).Msgf("Failed to process message: %s", streamID)
		}
	}(msg)
}

func (d *DeadLetterConsumer) fetchMessages(ctx context.Context, count int64) ([]redis.XMessage, error) {
	stream := buildDeadLetterKey(d.queue.DeadLetterName, "")
	result, err := d.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream, "0"},
		Count:   count,
		Block:   d.queue.LongPollingTime,
	}).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, redis.Nil
	}

	return result[0].Messages, nil
}

func (d *DeadLetterConsumer) processMessage(ctx context.Context, msg Message, streamID string) error {
	if d.processing == nil {
		err := errors.New("processing function is nil")
		log.Error().Err(err).Msg("Failed to process message")
		return err
	}

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), d.queue.BlockTime)
	defer cancel()

	r := make(chan error, 1)
	go func() {
		r <- d.processing(msg)
	}()

	select {
	case err := <-r:
		if err != nil {
			log.Error().Err(err).Msgf("Message %s processed with error", streamID)
			return d.handleFailure(ctx, msg, streamID, err)
		}

		_, err = d.client.XDel(ctx, buildDeadLetterKey(d.queue.DeadLetterName, ""), streamID).Result()
		if err != nil {
			log.Error().Err(err).Msgf("Failed to delete message %s", streamID)
		}
		log.Info().Msgf("Message %s processed successfully", streamID)
	case <-ctxWithTimeout.Done():
		log.Error().Err(ctxWithTimeout.Err()).Msgf("Message %s processing timed out", streamID)
		return d.handleFailure(ctx, msg, streamID, ctxWithTimeout.Err())
	}

	return nil
}

func (d *DeadLetterConsumer) handleFailure(ctx context.Context, msg Message, id string, err error) error {
	log.Debug().Err(err).Msgf("Handling failure for message %s", id)

	pendingResult, err := d.getPendingResult(ctx, id)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to get pending result for message %s", id)
		return err
	}

	for _, pending := range pendingResult {
		if pending.ID == id && pending.RetryCount >= d.retryCount {
			if d.finalFailurePattern == AbandonPattern {
				return d.acknowledgeAndWarn(ctx, msg, id)
			}
			return d.requeueMessage(ctx, msg, id)
		}
	}

	return nil
}

func (d *DeadLetterConsumer) getPendingResult(ctx context.Context, id string) ([]redis.XPendingExt, error) {
	return d.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: buildDeadLetterKey(d.queue.DeadLetterName, ""),
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
}

func (d *DeadLetterConsumer) requeueMessage(ctx context.Context, msg Message, id string) error {
	_, err := d.client.XAdd(ctx, &redis.XAddArgs{
		Stream: buildDeadLetterKey(d.queue.DeadLetterName, ""),
		Values: msg.Map(),
	}).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to requeue message %s", id)
		return err
	}

	_, err = d.client.XDel(ctx, buildDeadLetterKey(d.queue.DeadLetterName, ""), id).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete message %s after requeue", id)
		return err
	}

	log.Info().Msgf("Message %s requeued successfully", id)
	return nil
}

func (d *DeadLetterConsumer) acknowledgeAndWarn(ctx context.Context, msg Message, id string) error {
	_, err := d.client.XDel(ctx, buildDeadLetterKey(d.queue.DeadLetterName, ""), id).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete message %s", id)
		return err
	}

	log.Warn().Msgf("Message %s reached max retries and was acknowledged without further processing", id)
	return nil
}

func (d *DeadLetterConsumer) parseMessage(message redis.XMessage) (Message, string, error) {
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
