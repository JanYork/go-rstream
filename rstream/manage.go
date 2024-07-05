package rstream

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

const keyPrefix = "rstream:queue:"

// Manager handles the creation and management of queues
type Manager struct {
	client *redis.Client
}

func NewManager(client *redis.Client) *Manager {
	return &Manager{client: client}
}

// TODO：事务

func (m *Manager) CreateQueue(ctx context.Context, queue *Queue) (*GroupManager, *Queue, error) {
	metaKey, streamKey := buildKeys(queue.Name)

	exists, err := m.client.Exists(ctx, streamKey).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to check if stream %s exists", queue.Name)
		return nil, nil, err
	}

	var groupManager = NewGroupManager(m.client, queue)

	if exists == 0 {
		err = groupManager.CreateGroup(ctx, queue.DefaultGroup, "")

		if err != nil {
			log.Error().Err(err).Msgf("Failed to create default group for queue %s", queue.Name)
			return nil, nil, err
		}
	}

	deadLetterKey := buildStreamKey(queue.DeadLetterStream)

	exists, err = m.client.Exists(ctx, deadLetterKey).Result()
	if err != nil {
		m.client.Del(ctx, streamKey)
		log.Error().Err(err).Msgf("Failed to check if dead letter stream %s exists", queue.DeadLetterStream)
		return nil, nil, err
	}

	if exists == 0 {
		// 如果死信队列不存在，创建死信队列
		err = groupManager.CreateDeadLetterGroup(ctx, queue.DefaultGroup, "")

		if err != nil {
			m.client.Del(ctx, streamKey)
			log.Error().Err(err).Msgf("Failed to create dead letter group for queue %s", queue.Name)
			return nil, nil, err
		}
	}

	metadata := map[string]interface{}{
		"name":               queue.Name,
		"fifo":               queue.FIFO,
		"default_group":      queue.DefaultGroup,
		"block_time":         queue.BlockTime,
		"retry_count":        queue.RetryCount,
		"dead_letter_stream": queue.DeadLetterStream,
		"long_polling_time":  queue.LongPollingTime,
		"max_length":         queue.MaxLength,
		"max_consumers":      queue.MaxConsumers,
		"max_groups":         queue.MaxGroups,
		"max_concurrency":    queue.MaxConcurrency,
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		m.client.Del(ctx, streamKey)
		m.client.Del(ctx, deadLetterKey)
		log.Error().Err(err).Msgf("Failed to marshal metadata for queue %s", queue.Name)
		return nil, nil, err
	}

	err = m.client.Set(ctx, metaKey, metadataJSON, 0).Err()

	if err != nil {
		m.client.Del(ctx, streamKey)
		log.Error().Err(err).Msgf("Failed to set metadata for queue %s", queue.Name)
		return nil, nil, err
	}

	return groupManager, queue, nil
}

func (m *Manager) DeleteQueue(ctx context.Context, name string) error {
	metaKey, streamKey := buildKeys(name)

	err := m.client.Del(ctx, metaKey).Err()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete metadata for queue %s", name)
		return err
	}

	err = m.client.Del(ctx, streamKey).Err()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete stream for queue %s", name)
		return err
	}

	return nil
}

// buildKeys builds the keys for the queue,
// returns the meta key and the stream key
func buildKeys(name string) (string, string) {
	metaKey := keyPrefix + name + ":meta"
	streamKey := keyPrefix + name
	return metaKey, streamKey
}

// TODO:死信队列额外

func buildStreamKey(name string) string {
	return keyPrefix + name
}

func buildMetaKey(name string) string {
	return keyPrefix + name + ":meta"
}
