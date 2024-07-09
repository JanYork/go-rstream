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
	client      *redis.Client
	queue       *Queue                                              // 队列
	group       string                                              // 消费组
	consumer    string                                              // 消费者
	tag         string                                              // 消费者标签
	ctx         map[string]interface{}                              // 消费者关联信息上下文
	mu          sync.Mutex                                          // 互斥锁
	wg          sync.WaitGroup                                      // 等待组
	processing  func(Message, string, map[string]interface{}) error // 处理消息的函数
	currentWait time.Duration                                       // 获取消息等待时间
	paused      bool                                                // 暂停标志
	stop        bool                                                // 停止标志
}

func NewConsumer(client *redis.Client, queue *Queue, group, consumer, tag string, ctx map[string]interface{}, processing func(Message, string, map[string]interface{}) error) *Consumer {
	return &Consumer{
		client:      client,
		queue:       queue,
		group:       group,
		consumer:    consumer,
		tag:         tag,
		ctx:         ctx,
		processing:  processing,
		currentWait: 0,
		paused:      false,
		stop:        false,
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
	pendingInfo, err := c.client.XPending(ctx, buildQueueKey(c.queue.Name), c.group).Result()
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
		Streams:  []string{buildQueueKey(c.queue.Name), ">"},
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

		_, err = c.client.XAck(ctx, buildQueueKey(c.queue.Name), c.group, id).Result()
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
			if err := c.sendToDeadLetterQueue(ctx, msg, id); err != nil {
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
		Stream:   buildQueueKey(c.queue.Name),
		Group:    c.group,
		Start:    id,
		End:      id,
		Count:    1,
		Consumer: c.consumer,
	}).Result()
}

func (c *Consumer) sendToDeadLetterQueue(ctx context.Context, msg Message, id string) error {
	deadLetterStreamKey := buildDeadLetterQueueKey(c.queue.DeadLetterName, c.group)

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

	_, err = c.client.XAck(ctx, buildQueueKey(c.queue.Name), c.group, id).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to acknowledge message %s", id)
		return err
	}

	return nil
}

func (c *Consumer) retryMessage(ctx context.Context, id string) error {
	log.Debug().Msgf("Retrying message %s", id)
	claimResult, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   buildQueueKey(c.queue.Name),
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

	uwt := func(t time.Duration) {
		maxWait := 3 * time.Second
		if t < maxWait {
			c.currentWait += t
		}
	}

	if err != nil {
		log.Error().Err(err).Msg("Failed to get active workers")
		c.currentWait = 1 * time.Second
		return 0, err
	}

	if c.queue.FIFO {
		if activeWorkers >= 1 {
			log.Debug().Msg("FIFO queue has active workers")
			uwt(300 * time.Millisecond)
			return 0, nil
		}

		return 1, nil
	}

	if activeWorkers >= c.queue.MaxConcurrency {
		log.Debug().Msg("Max concurrency reached")
		uwt(300 * time.Millisecond)
		return 0, nil
	}

	if c.currentWait > 0 {
		c.currentWait = 0
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
	if err := c.checkConsumerLimit(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to start consumer")
		return err
	}

	for {
		c.mu.Lock()
		if c.stop {
			c.mu.Unlock()
			break
		}
		paused := c.paused
		c.mu.Unlock()

		if paused {
			time.Sleep(3 * time.Second)
			continue
		}

		remaining, err := c.checkActiveWorkers(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to check active workers")
			time.Sleep(c.currentWait)
			continue
		}

		if remaining == 0 {
			time.Sleep(c.currentWait)
			continue
		}

		messages, err := c.fetchMessages(ctx, remaining)

		if err != nil && !errors.Is(err, redis.Nil) {
			log.Error().Err(err).Msg("Failed to fetch messages")
			return err
		}

		for _, stream := range messages {
			log.Debug().Msgf("Processing stream %s", stream.Stream)
			c.processStream(ctx, stream)
		}
	}

	log.Info().Msgf("Consumer %s stopped", c.consumer)
	return nil
}

func (c *Consumer) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
}

func (c *Consumer) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
}

func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stop = true
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	c.Stop()

	c.mu.Lock()
	defer c.mu.Unlock()

	// 注销消费者，将pending消息放入死信队列并ACK
	pendingMsgs, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: buildQueueKey(c.queue.Name),
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	if err != nil {
		log.Error().Err(err).Msg("Failed to get pending messages during shutdown")
		return err
	}

	for _, pmsg := range pendingMsgs {
		// 获取消息的详细内容
		msgDetails, err := c.client.XRange(ctx, buildQueueKey(c.queue.Name), pmsg.ID, pmsg.ID).Result()
		if err != nil {
			log.Error().Err(err).Msgf("Failed to get message details for %s during shutdown", pmsg.ID)
			continue
		}

		// 确保只获取一条消息
		if len(msgDetails) != 1 {
			log.Error().Err(err).Msgf("Unexpected number of messages returned for %s during shutdown", pmsg.ID)
			continue
		}

		// 解析消息
		msg, id, err := c.parseMessage(msgDetails[0])
		if err != nil {
			log.Error().Err(err).Msgf("Failed to parse pending message %s", pmsg.ID)
			continue
		}

		if err := c.sendToDeadLetterQueue(ctx, msg, id); err != nil {
			log.Error().Err(err).Msgf("Failed to send message %s to dead letter queue", id)
			continue
		}

		if _, err := c.client.XAck(ctx, buildQueueKey(c.queue.Name), c.group, id).Result(); err != nil {
			log.Error().Err(err).Msgf("Failed to acknowledge message %s during shutdown", id)
		}
	}
	return nil
}

func (c *Consumer) checkConsumerLimit(ctx context.Context) error {
	maxConsumerNum := c.queue.MaxConsumers

	currentConsumers, err := c.getCurrentConsumers(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get current consumers")
		return err
	}
	if currentConsumers >= maxConsumerNum {
		err = errors.New("the number of consumers has reached the maximum limit")
		log.Error().Err(err).Msg("Failed to start consumer: maximum number of consumers reached")
		return err
	}
	return nil
}

func (c *Consumer) getCurrentConsumers(ctx context.Context) (int64, error) {
	consumers, err := c.client.XInfoConsumers(ctx, buildQueueKey(c.queue.Name), c.group).Result()
	if err != nil {
		return 0, err
	}
	return int64(len(consumers)), nil
}
