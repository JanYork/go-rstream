package rstream

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"time"
)

type Queue struct {
	Client           *redis.Client
	Name             string        // 队列名称
	DefaultGroup     string        // 默认消费组
	BlockTime        time.Duration // 阻塞时间(超时时间)
	FIFO             bool          // 顺序消费
	RetryCount       int64         // 重试次数
	DeadLetterStream string        // 死信队列
	LongPollingTime  time.Duration // 长轮训时间
	MaxLength        int64         // 最大长度
	MaxConsumers     int64         // 最大消费者数
	MaxGroups        int64         // 最大消费组数
	MaxConcurrency   int64         // 最大并发数(异步可用)
}

func NewQueue(client *redis.Client, name string) *Queue {
	return &Queue{
		Client: client,
		Name:   name,
	}
}

func (q *Queue) SyncQueue(ctx context.Context, queue *Queue) error {
	log.Debug().Msgf("Syncing queue %s", q.Name)

	metaKey := buildMetaKey(q.Name)

	meta, err := q.Client.Get(ctx, metaKey).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to get metadata for queue %s", q.Name)
		return err
	}

	var metadata map[string]interface{}
	if err = json.Unmarshal([]byte(meta), &metadata); err != nil {
		return err
	}

	updateMetadata(metadata, queue)

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to marshal metadata for queue %s", q.Name)
		return err
	}

	_, err = q.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, metaKey, metadataJSON, 0)
		updateQueueFields(q, queue)
		return nil
	})

	if err != nil {
		log.Error().Err(err).Msgf("Failed to sync queue %s", q.Name)
		return err
	}

	return nil
}

func updateMetadata(metadata map[string]interface{}, queue *Queue) {
	if queue.MaxConcurrency != 0 {
		metadata["max_concurrency"] = queue.MaxConcurrency
	}
	if queue.LongPollingTime != 0 {
		metadata["long_polling_time"] = queue.LongPollingTime
	}
	if queue.BlockTime != 0 {
		metadata["block_time"] = queue.BlockTime
	}
	if queue.RetryCount != 0 {
		metadata["retry_count"] = queue.RetryCount
	}
}

func updateQueueFields(q *Queue, queue *Queue) {
	if queue.MaxConcurrency != 0 {
		q.MaxConcurrency = queue.MaxConcurrency
	}
	if queue.LongPollingTime != 0 {
		q.LongPollingTime = queue.LongPollingTime
	}
	if queue.BlockTime != 0 {
		q.BlockTime = queue.BlockTime
	}
	if queue.RetryCount != 0 {
		q.RetryCount = queue.RetryCount
	}
}
