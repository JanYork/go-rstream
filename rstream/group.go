package rstream

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
)

// GroupManager handles the creation and management of consumer groups
type GroupManager struct {
	client             *redis.Client
	queue              *Queue
	destroyMovePending bool // 注销组时是否将未处理的消息转移到死信队列
}

// NewGroupManager creates a new GroupManager
func NewGroupManager(client *redis.Client, queue *Queue) *GroupManager {
	return &GroupManager{
		client: client,
		queue:  queue,
	}
}

func (g *GroupManager) SwitchDestroyMovePending() {
	g.destroyMovePending = !g.destroyMovePending
}

// CreateGroup creates a consumer group if it does not already exist
func (g *GroupManager) CreateGroup(ctx context.Context, group string, start string) error {
	stream := buildStreamKey(g.queue.Name)

	log.Debug().Msgf("Creating group %s, start: %s, stream: %s", group, start, stream)

	// Check if it meets the queue configuration rules
	if err := g.checkMaxGroups(ctx, stream); err != nil {
		return err
	}

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
	deadLetterStream := buildDeadLetterKey(g.queue.DeadLetterName)

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

// DestroyGroup removes a consumer group, and returns the number of pending messages, the number of consumers, and the group's next offset
func (g *GroupManager) DestroyGroup(ctx context.Context, group string) (int64, int64, string, error) {
	stream := buildStreamKey(g.queue.Name)

	log.Debug().Msgf("Removing group %s, stream: %s", group, stream)

	// Get the number of pending messages
	pending, err := g.client.XPending(ctx, stream, group).Result()
	if err != nil {
		logError("Failed to get pending messages for group %s", group, err)
		return 0, 0, "", err
	}

	// Get the number of consumers
	consumers, err := g.client.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		logError("Failed to get consumers for group %s", group, err)
		return 0, 0, "", err
	}

	// Get the group's next offset
	nextOffset, err := g.getGroupNextOffset(ctx, stream, group)
	if err != nil {
		logError("Failed to get next offset for group %s", group, err)
		return 0, 0, "", err
	}

	// If enabled, move pending messages to the dead letter queue
	if g.destroyMovePending {
		err := g.movePendingMessagesToDeadLetter(ctx, stream, group, pending.Count)
		if err != nil {
			return 0, 0, "", err
		}
	}

	// Destroy the group
	_, err = g.client.XGroupDestroy(ctx, stream, group).Result()
	if err != nil {
		logError("Failed to remove group %s", group, err)
		return 0, 0, "", err
	}

	return pending.Count, int64(len(consumers)), nextOffset, nil
}

func (g *GroupManager) getGroupNextOffset(ctx context.Context, stream, group string) (string, error) {
	groupInfoList, err := g.client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return "", err
	}

	for _, groupInfo := range groupInfoList {
		if groupInfo.Name == group {
			return groupInfo.LastDeliveredID, nil
		}
	}

	return "", errors.New("failed to get next offset or group does not exist")
}

func (g *GroupManager) movePendingMessagesToDeadLetter(ctx context.Context, stream, group string, count int64) error {
	pendingMsgs, err := g.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err != nil {
		logError("Failed to get pending messages for group %s", group, err)
		return err
	}

	for _, pmsg := range pendingMsgs {
		err := g.moveMessageToDeadLetter(ctx, stream, group, pmsg.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *GroupManager) moveMessageToDeadLetter(ctx context.Context, stream, group, msgID string) error {
	msgDetails, err := g.client.XRange(ctx, stream, msgID, msgID).Result()
	if err != nil {
		logError("Failed to get message details for %s", msgID, err)
		return err
	}

	if len(msgDetails) != 1 {
		return errors.New("unexpected number of messages returned")
	}

	_, err = g.client.XAdd(ctx, &redis.XAddArgs{
		Stream: buildDeadLetterKey(g.queue.DeadLetterName),
		Values: msgDetails[0].Values,
	}).Result()
	if err != nil {
		logError("Failed to add message %s to dead letter queue", msgID, err)
		return err
	}

	_, err = g.client.XAck(ctx, stream, group, msgID).Result()
	if err != nil {
		logError("Failed to ACK message %s", msgID, err)
		return err
	}

	return nil
}

func (g *GroupManager) checkMaxGroups(ctx context.Context, stream string) error {
	groups, err := g.client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		log.Error().Err(err).Msgf("Failed to get groups for stream %s", stream)
		return err
	}

	if int64(len(groups)) >= g.queue.MaxGroups {
		err = errors.New("the number of consumer groups has reached the maximum limit")
		log.Error().Err(err).Msg("Failed to create group: maximum number of consumer groups reached")
		return err
	}

	return nil
}

func logError(message, group string, err error) {
	log.Error().Err(err).Msgf(message, group)
}
