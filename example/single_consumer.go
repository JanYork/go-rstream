package example

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"rstream/rstream"
	"time"
)

func SingleConsumer(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "singleConsumerQueue",
		DefaultGroup:    "singleConsumerGroup",
		BlockTime:       15 * time.Second,
		FIFO:            false,
		RetryCount:      3,
		DeadLetterName:  "singleConsumerDeadLetter",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10,
	}

	groupKey := "scGroup"
	manager := rstream.NewGroupManager(queue)
	ctx := context.Background()

	err := manager.CreateGroup(ctx, groupKey, "")
	if err != nil {
		log.Printf("Failed to create group: %v", err)
		return
	}
	go startSingleConsumer(client, queue, groupKey, "single-consumer")
	startSingleProducer(queue, 3)
}

func startSingleProducer(queue *rstream.Queue, messageCount int) {
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

func startSingleConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
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

//{"level":"debug","time":"2024-07-09T17:01:19+08:00","message":"Creating groupName scGroup, start: , stream: rstream:queue:singleConsumerQueue"}
//{"level":"debug","time":"2024-07-09T17:01:19+08:00","message":"Published message to stream rstream:queue:singleConsumerQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T17:01:19+08:00","message":"Processing stream rstream:queue:singleConsumerQueue"}
//2024/07/09 17:01:19 group: scGroup, consumer: single-consumer, message: msg-0 0 2024-07-09 17:01:19.949465 +0800 CST
//{"level":"debug","time":"2024-07-09T17:01:20+08:00","message":"Published message to stream rstream:queue:singleConsumerQueue, message: msg-1"}
//{"level":"debug","time":"2024-07-09T17:01:20+08:00","message":"Processing stream rstream:queue:singleConsumerQueue"}
//2024/07/09 17:01:20 group: scGroup, consumer: single-consumer, message: msg-1 1 2024-07-09 17:01:20.950474 +0800 CST
//{"level":"debug","time":"2024-07-09T17:01:21+08:00","message":"Published message to stream rstream:queue:singleConsumerQueue, message: msg-2"}
//{"level":"debug","time":"2024-07-09T17:01:21+08:00","message":"Processing stream rstream:queue:singleConsumerQueue"}
//2024/07/09 17:01:21 group: scGroup, consumer: single-consumer, message: msg-2 2 2024-07-09 17:01:21.952732 +0800 CST
//{"level":"debug","time":"2024-07-09T17:01:22+08:00","message":"Message msg-0 processed successfully"}
//{"level":"debug","time":"2024-07-09T17:01:23+08:00","message":"Message msg-1 processed successfully"}
//{"level":"debug","time":"2024-07-09T17:01:24+08:00","message":"Message msg-2 processed successfully"}
