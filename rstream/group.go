package rstream

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"time"
)

type GroupManager struct {
	queue              *Queue
	destroyMovePending bool // 注销组时是否将未处理的消息转移到死信队列
	groups             map[string]*Group
	deadLetterGroups   map[string]*Group
}

// NewGroupManager Creates a new GroupManager
func NewGroupManager(queue *Queue) *GroupManager {
	return &GroupManager{
		queue:            queue,
		groups:           make(map[string]*Group),
		deadLetterGroups: make(map[string]*Group),
	}
}

func (g *GroupManager) SwitchDestroyMovePending() {
	g.destroyMovePending = !g.destroyMovePending
}

// CreateGroup Creates a consumer group if it does not already exist
func (g *GroupManager) CreateGroup(ctx context.Context, groupName string, start string) error {
	stream := buildQueueKey(g.queue.Name)

	log.Debug().Msgf("Creating groupName %s, start: %s, stream: %s", groupName, start, stream)

	// Check if it meets the queue configuration rules
	if err := g.checkMaxGroups(ctx, stream); err != nil {
		return err
	}

	if start == "" {
		start = "$"
	}

	group := &Group{
		Name:      groupName,
		Queue:     g.queue,
		CreatedAt: time.Now(),
	}

	// Check if the groupName already exists
	_, err := g.queue.Client.XGroupCreateMkStream(ctx, stream, group.Name, start).Result()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			// The groupName already exists, no need to create it again
			return nil
		}
		log.Error().Err(err).Msgf("Failed to create groupName %s", group.Name)
		return err
	}

	g.groups[groupName] = group

	return nil
}

// CreateDeadLetterGroup Creates a dead letter consumer group if it does not already exist
func (g *GroupManager) CreateDeadLetterGroup(ctx context.Context, groupName string) error {
	stream := buildQueueKey(g.queue.DeadLetterName)
	group := &Group{
		Name:      groupName,
		Queue:     g.queue,
		CreatedAt: time.Now(),
	}

	// Check if the groupName already exists
	_, err := g.queue.Client.XGroupCreateMkStream(ctx, stream, group.Name, "$").Result()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			// The groupName already exists, no need to create it again
			return nil
		}
		log.Error().Err(err).Msgf("Failed to create groupName %s", group.Name)
		return err
	}

	g.deadLetterGroups[groupName] = group

	return nil
}

// CreateGroupWithDeadLetter Creates a consumer group and a dead letter consumer group if they do not already exist
func (g *GroupManager) CreateGroupWithDeadLetter(ctx context.Context, groupName string, start string) error {
	err := g.CreateGroup(ctx, groupName, start)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to create groupName %s", groupName)
		return err
	}

	err = g.CreateDeadLetterGroup(ctx, groupName)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to create dead letter groupName %s", groupName)
		return err
	}

	return nil
}

// DestroyGroup Removes a consumer group, and returns the number of pending messages, the number of consumers, and the group's next offset
func (g *GroupManager) DestroyGroup(ctx context.Context, groupName string) (int64, int64, string, error) {
	group, exists := g.groups[groupName]
	if !exists {
		return 0, 0, "", errors.New("group does not exist")
	}

	// Get the number of pending messages
	pendingCount, consumersCount, nextOffset, err := group.Destroy(ctx, g.destroyMovePending)
	if err != nil {
		return 0, 0, "", err
	}

	delete(g.groups, groupName)

	return pendingCount, consumersCount, nextOffset, nil
}

func (g *GroupManager) checkMaxGroups(ctx context.Context, stream string) error {
	groups, err := g.queue.Client.XInfoGroups(ctx, stream).Result()
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

type Group struct {
	Name      string
	Queue     *Queue
	CreatedAt time.Time
}

// Destroy Removes the consumer group, optionally moving pending messages to the dead letter queue
func (g *Group) Destroy(ctx context.Context, movePending bool) (int64, int64, string, error) {
	streamKey := buildQueueKey(g.Queue.Name)

	log.Debug().Msgf("Removing groupName %s, stream: %s", g.Name, streamKey)

	// Get the number of pending messages
	pending, err := g.Queue.Client.XPending(ctx, streamKey, g.Name).Result()
	if err != nil {
		logError("Failed to get pending messages for groupName %s", g.Name, err)
		return 0, 0, "", err
	}

	// Get the number of consumers
	consumers, err := g.Queue.Client.XInfoConsumers(ctx, streamKey, g.Name).Result()
	if err != nil {
		logError("Failed to get consumers for groupName %s", g.Name, err)
		return 0, 0, "", err
	}

	// Get the groupName's next offset
	nextOffset, err := g.getGroupNextOffset(ctx)
	if err != nil {
		logError("Failed to get next offset for groupName %s", g.Name, err)
		return 0, 0, "", err
	}

	// If enabled, move pending messages to the dead letter queue
	if movePending {
		err := g.movePendingMessagesToDeadLetter(ctx, pending.Count)
		if err != nil {
			return 0, 0, "", err
		}
	}

	// Destroy the groupName
	_, err = g.Queue.Client.XGroupDestroy(ctx, streamKey, g.Name).Result()
	if err != nil {
		logError("Failed to remove groupName %s", g.Name, err)
		return 0, 0, "", err
	}

	return pending.Count, int64(len(consumers)), nextOffset, nil
}

func (g *Group) getGroupNextOffset(ctx context.Context) (string, error) {
	groupInfoList, err := g.Queue.Client.XInfoGroups(ctx, buildQueueKey(g.Queue.Name)).Result()
	if err != nil {
		return "", err
	}

	for _, groupInfo := range groupInfoList {
		if groupInfo.Name == g.Name {
			return groupInfo.LastDeliveredID, nil
		}
	}

	return "", errors.New("failed to get next offset or groupName does not exist")
}

func (g *Group) movePendingMessagesToDeadLetter(ctx context.Context, count int64) error {
	streamKey := buildQueueKey(g.Queue.Name)
	pendingMsgs, err := g.Queue.Client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  g.Name,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err != nil {
		logError("Failed to get pending messages for groupName %s", g.Name, err)
		return err
	}

	for _, pmsg := range pendingMsgs {
		err := g.moveMessageToDeadLetter(ctx, pmsg.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Group) moveMessageToDeadLetter(ctx context.Context, msgID string) error {
	streamKey := buildQueueKey(g.Queue.Name)
	msgDetails, err := g.Queue.Client.XRange(ctx, streamKey, msgID, msgID).Result()
	if err != nil {
		logError("Failed to get message details for %s", msgID, err)
		return err
	}

	if len(msgDetails) != 1 {
		return errors.New("unexpected number of messages returned")
	}

	_, err = g.Queue.Client.XAdd(ctx, &redis.XAddArgs{
		Stream: buildDeadLetterQueueKey(g.Queue.DeadLetterName, g.Name),
		Values: msgDetails[0].Values,
	}).Result()
	if err != nil {
		logError("Failed to add message %s to dead letter queue", msgID, err)
		return err
	}

	_, err = g.Queue.Client.XAck(ctx, streamKey, g.Name, msgID).Result()
	if err != nil {
		logError("Failed to ACK message %s", msgID, err)
		return err
	}

	return nil
}

func logError(message, group string, err error) {
	log.Error().Err(err).Msgf(message, group)
}

// RunScanPendingMessages Pending messages are scanned on a regular basis to transfer messages that have timed out but are not processed to a dead-letter queue
func (g *Group) RunScanPendingMessages(ctx context.Context) error {
	streamKey := buildQueueKey(g.Queue.Name)
	blockTime := g.Queue.BlockTime
	thresholdTime := blockTime + 30*time.Second

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pendingMsgs, err := g.Queue.Client.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: streamKey,
				Group:  g.Name,
				Start:  "-",
				End:    "+",
				Idle:   thresholdTime,
				Count:  100,
			}).Result()
			if err != nil {
				logError("Failed to get pending messages for groupName %s", g.Name, err)
				return err
			}

			for _, pmsg := range pendingMsgs {
				err := g.moveMessageToDeadLetter(ctx, pmsg.ID)
				if err != nil {
					return err
				}
			}
		}
	}
}
