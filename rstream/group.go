package rstream

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

// GroupManager handles the creation and management of consumer groups
type GroupManager struct {
	client *redis.Client
	queue  *Queue
}

// NewGroupManager creates a new GroupManager
func NewGroupManager(client *redis.Client, queue *Queue) *GroupManager {
	return &GroupManager{
		client: client,
		queue:  queue,
	}
}

// CreateGroup creates a consumer group if it does not already exist
func (g *GroupManager) CreateGroup(ctx context.Context, group string, start string) error {
	stream := buildStreamKey(g.queue.Name)

	log.Debug().Msgf("Creating group %s, start: %s, stream: %s", group, start, stream)

	// TODO：检查是否符合队列配置规约

	if start == "" {
		start = "$"
	}

	// Check if the group already exists
	_, err := g.client.XGroupCreateMkStream(ctx, stream, group, start).Result()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			// The group already exists, no need to create it again
			return nil
		}
		log.Error().Err(err).Msgf("Failed to create group %s", group)
		return err
	}
	return nil
}

// CreateDeadLetterGroup creates a dead letter consumer group if it does not already exist
func (g *GroupManager) CreateDeadLetterGroup(ctx context.Context, group string, start string) error {
	deadLetterStream := buildStreamKey(g.queue.DeadLetterStream)

	log.Debug().Msgf("Creating dead letter group %s, start: %s, stream: %s", group, start, deadLetterStream)

	if start == "" {
		start = "$"
	}

	// Check if the group already exists
	_, err := g.client.XGroupCreateMkStream(ctx, deadLetterStream, group, start).Result()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			// The group already exists, no need to create it again
			return nil
		}
		log.Error().Err(err).Msgf("Failed to create dead letter group %s", group)
		return err
	}
	return nil
}
