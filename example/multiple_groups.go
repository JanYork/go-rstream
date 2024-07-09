package example

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"rstream/rstream"
	"time"
)

func MultipleGroups(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "multipleGroupsQueue",
		DefaultGroup:    "multipleGroupsGroup",
		BlockTime:       5 * time.Second,
		FIFO:            false,
		RetryCount:      3,
		DeadLetterName:  "multipleGroupsDeadLetter",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10,
	}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		group := fmt.Sprintf("tmg-group-%d", i)
		manager := rstream.NewGroupManager(queue)
		err := manager.CreateGroup(ctx, group, "")
		if err != nil {
			log.Printf("Failed to create group: %v", err)
		}
		go startMultipleGroupsConsumer(client, queue, group, fmt.Sprintf("consumer-%d", i))
	}
	startMultipleGroupsProducer(queue, 3)
}

func startMultipleGroupsProducer(queue *rstream.Queue, messageCount int) {
	producer := rstream.NewProducer(queue)
	for i := 0; i < messageCount; i++ {
		message := rstream.Message{
			ID:      fmt.Sprintf("msg-%d", i),
			Payload: []byte(fmt.Sprintf("%d", i)),
			Created: time.Now(),
		}
		ctx := context.Background()

		err := producer.Publish(ctx, message)
		if err != nil {
			log.Printf("Failed to publish message: %v", err)
		}

		time.Sleep(1 * time.Second)
	}
}

func startMultipleGroupsConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
	processMessage := func(msg rstream.Message, tag string, ctx map[string]interface{}) error {
		log.Printf("group: %s, consumer: %s, message: %s", group, consumerName, msg.String())
		time.Sleep(3 * time.Second)
		return nil
	}

	consumer := rstream.NewConsumer(client, queue, group, consumerName, "tag", nil, processMessage)
	ctx := context.Background()

	err := consumer.Consume(ctx)
	if err != nil {
		log.Printf("Consumer %s failed: %v", consumerName, err)
	}
}

//{"level":"debug","time":"2024-07-09T16:56:33+08:00","message":"Creating groupName tmg-group-0, start: , stream: rstream:queue:multipleGroupsQueue"}
//{"level":"debug","time":"2024-07-09T16:56:33+08:00","message":"Creating groupName tmg-group-1, start: , stream: rstream:queue:multipleGroupsQueue"}
//{"level":"debug","time":"2024-07-09T16:56:33+08:00","message":"Published message to stream rstream:queue:multipleGroupsQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T16:56:33+08:00","message":"Processing stream rstream:queue:multipleGroupsQueue"}
//2024/07/09 16:56:33 group: tmg-group-0, consumer: consumer-0, message: msg-0 0 2024-07-09 16:56:33.297764 +0800 CST
//{"level":"debug","time":"2024-07-09T16:56:33+08:00","message":"Processing stream rstream:queue:multipleGroupsQueue"}
//2024/07/09 16:56:33 group: tmg-group-1, consumer: consumer-1, message: msg-0 0 2024-07-09 16:56:33.297764 +0800 CST
//{"level":"debug","time":"2024-07-09T16:56:34+08:00","message":"Processing stream rstream:queue:multipleGroupsQueue"}
//2024/07/09 16:56:34 group: tmg-group-1, consumer: consumer-1, message: msg-1 1 2024-07-09 16:56:34.299295 +0800 CST
//{"level":"debug","time":"2024-07-09T16:56:34+08:00","message":"Processing stream rstream:queue:multipleGroupsQueue"}
//{"level":"debug","time":"2024-07-09T16:56:34+08:00","message":"Published message to stream rstream:queue:multipleGroupsQueue, message: msg-1"}
//2024/07/09 16:56:34 group: tmg-group-0, consumer: consumer-0, message: msg-1 1 2024-07-09 16:56:34.299295 +0800 CST
//{"level":"debug","time":"2024-07-09T16:56:35+08:00","message":"Processing stream rstream:queue:multipleGroupsQueue"}
//{"level":"debug","time":"2024-07-09T16:56:35+08:00","message":"Published message to stream rstream:queue:multipleGroupsQueue, message: msg-2"}
//2024/07/09 16:56:35 group: tmg-group-0, consumer: consumer-0, message: msg-2 2 2024-07-09 16:56:35.301192 +0800 CST
//{"level":"debug","time":"2024-07-09T16:56:35+08:00","message":"Processing stream rstream:queue:multipleGroupsQueue"}
//2024/07/09 16:56:35 group: tmg-group-1, consumer: consumer-1, message: msg-2 2 2024-07-09 16:56:35.301192 +0800 CST
//{"level":"debug","time":"2024-07-09T16:56:36+08:00","message":"Message msg-0 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:56:36+08:00","message":"Message msg-0 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:56:37+08:00","message":"Message msg-1 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:56:37+08:00","message":"Message msg-1 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:56:38+08:00","message":"Message msg-2 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:56:38+08:00","message":"Message msg-2 processed successfully"}
