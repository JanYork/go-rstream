package example

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"rstream/rstream"
	"time"
)

func MultipleConsumers(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "multipleConsumersQueue",
		DefaultGroup:    "multipleConsumersGroup",
		BlockTime:       5 * time.Second,
		FIFO:            false,
		RetryCount:      3,
		DeadLetterName:  "multipleConsumersDeadLetter",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10,
	}

	manager := rstream.NewGroupManager(queue)
	groupKey := "mcGroup"
	ctx := context.Background()

	err := manager.CreateGroup(ctx, groupKey, "")
	if err != nil {
		log.Printf("Failed to create group: %v", err)
		return
	}

	for i := 0; i < 3; i++ {
		go startMultipleConsumersConsumer(client, queue, groupKey, fmt.Sprintf("consumer-%d", i))
	}
	startMultipleConsumersProducer(queue, 3)
}

func startMultipleConsumersProducer(queue *rstream.Queue, messageCount int) {
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

func startMultipleConsumersConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
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

//{"level":"debug","time":"2024-07-09T16:59:24+08:00","message":"Creating groupName mcGroup, start: , stream: rstream:queue:multipleConsumersQueue"}
//{"level":"debug","time":"2024-07-09T16:59:24+08:00","message":"Published message to stream rstream:queue:multipleConsumersQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T16:59:24+08:00","message":"Processing stream rstream:queue:multipleConsumersQueue"}
//2024/07/09 16:59:24 group: mcGroup, consumer: consumer-0, message: msg-0 0 2024-07-09 16:59:24.578173 +0800 CST
//{"level":"debug","time":"2024-07-09T16:59:25+08:00","message":"Processing stream rstream:queue:multipleConsumersQueue"}
//{"level":"debug","time":"2024-07-09T16:59:25+08:00","message":"Published message to stream rstream:queue:multipleConsumersQueue, message: msg-1"}
//2024/07/09 16:59:25 group: mcGroup, consumer: consumer-1, message: msg-1 1 2024-07-09 16:59:25.578975 +0800 CST
//{"level":"debug","time":"2024-07-09T16:59:26+08:00","message":"Published message to stream rstream:queue:multipleConsumersQueue, message: msg-2"}
//{"level":"debug","time":"2024-07-09T16:59:26+08:00","message":"Processing stream rstream:queue:multipleConsumersQueue"}
//2024/07/09 16:59:26 group: mcGroup, consumer: consumer-0, message: msg-2 2 2024-07-09 16:59:26.580412 +0800 CST
//{"level":"debug","time":"2024-07-09T16:59:27+08:00","message":"Message msg-0 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:59:28+08:00","message":"Message msg-1 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:59:29+08:00","message":"Message msg-2 processed successfully"}
