package example

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"math/rand"
	"rstream/rstream"
	"time"
)

func AsyncProcessing(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "asyncProcessing",
		DefaultGroup:    "asyncProcessing",
		BlockTime:       5 * time.Second,
		FIFO:            false,
		RetryCount:      3,
		DeadLetterName:  "asyncProcessing",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10,
	}

	ctx := context.Background()

	manager := rstream.NewGroupManager(queue)

	err := manager.CreateGroup(ctx, queue.DefaultGroup, "")
	if err != nil {
		log.Printf("Failed to create group: %v", err)
		return
	}

	for i := 0; i < 3; i++ {
		go startAsyncConsumer(client, queue, queue.DefaultGroup, fmt.Sprintf("async-consumer-%d", i))
	}

	go startAsyncProducer(queue, 5)
}

func startAsyncProducer(queue *rstream.Queue, messageCount int) {
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

func startAsyncConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
	processMessage := func(msg rstream.Message, tag string, ctx map[string]interface{}) error {
		log.Printf("group: %s, consumer: %s, message: %s", group, consumerName, msg.String())
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Second)
		return nil
	}

	consumer := rstream.NewConsumer(client, queue, group, consumerName, "tag", nil, processMessage)
	ctx := context.Background()

	err := consumer.Consume(ctx)
	if err != nil {
		log.Printf("Consumer %s failed: %v", consumerName, err)
	}
}

//{"level":"debug","time":"2024-07-09T16:57:25+08:00","message":"Creating groupName asyncProcessing, start: , stream: rstream:queue:asyncProcessing"}
//{"level":"debug","time":"2024-07-09T16:57:25+08:00","message":"Published message to stream rstream:queue:asyncProcessing, message: msg-0"}
//{"level":"debug","time":"2024-07-09T16:57:25+08:00","message":"Processing stream rstream:queue:asyncProcessing"}
//2024/07/09 16:57:25 group: asyncProcessing, consumer: async-consumer-0, message: msg-0 0 2024-07-09 16:57:25.49225 +0800 CST
//{"level":"debug","time":"2024-07-09T16:57:26+08:00","message":"Processing stream rstream:queue:asyncProcessing"}
//{"level":"debug","time":"2024-07-09T16:57:26+08:00","message":"Published message to stream rstream:queue:asyncProcessing, message: msg-1"}
//2024/07/09 16:57:26 group: asyncProcessing, consumer: async-consumer-0, message: msg-1 1 2024-07-09 16:57:26.495143 +0800 CST
//{"level":"debug","time":"2024-07-09T16:57:27+08:00","message":"Published message to stream rstream:queue:asyncProcessing, message: msg-2"}
//{"level":"debug","time":"2024-07-09T16:57:27+08:00","message":"Processing stream rstream:queue:asyncProcessing"}
//2024/07/09 16:57:27 group: asyncProcessing, consumer: async-consumer-1, message: msg-2 2 2024-07-09 16:57:27.496221 +0800 CST
//{"level":"debug","time":"2024-07-09T16:57:28+08:00","message":"Processing stream rstream:queue:asyncProcessing"}
//{"level":"debug","time":"2024-07-09T16:57:28+08:00","message":"Published message to stream rstream:queue:asyncProcessing, message: msg-3"}
//2024/07/09 16:57:28 group: asyncProcessing, consumer: async-consumer-0, message: msg-3 3 2024-07-09 16:57:28.497266 +0800 CST
//{"level":"debug","time":"2024-07-09T16:57:29+08:00","message":"Message msg-1 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:57:29+08:00","message":"Published message to stream rstream:queue:asyncProcessing, message: msg-4"}
//{"level":"debug","time":"2024-07-09T16:57:29+08:00","message":"Processing stream rstream:queue:asyncProcessing"}
//2024/07/09 16:57:29 group: asyncProcessing, consumer: async-consumer-1, message: msg-4 4 2024-07-09 16:57:29.499224 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T16:57:30+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T16:57:30+08:00","message":"Handling failure for message 1720515445493-0"}
//{"level":"debug","time":"2024-07-09T16:57:30+08:00","message":"Retrying message 1720515445493-0"}
//{"level":"debug","time":"2024-07-09T16:57:30+08:00","message":"Message 1720515445493-0 reassigned successfully"}
//2024/07/09 16:57:30 group: asyncProcessing, consumer: async-consumer-0, message: msg-0 0 2024-07-09 16:57:25.49225 +0800 CST
//{"level":"debug","time":"2024-07-09T16:57:31+08:00","message":"Message msg-3 processed successfully"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T16:57:32+08:00","message":"Message msg-2 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T16:57:32+08:00","message":"Handling failure for message 1720515447496-0"}
//{"level":"debug","time":"2024-07-09T16:57:32+08:00","message":"Retrying message 1720515447496-0"}
//{"level":"debug","time":"2024-07-09T16:57:32+08:00","message":"Message 1720515447496-0 reassigned successfully"}
//2024/07/09 16:57:32 group: asyncProcessing, consumer: async-consumer-1, message: msg-2 2 2024-07-09 16:57:27.496221 +0800 CST
//{"level":"debug","time":"2024-07-09T16:57:32+08:00","message":"Message msg-4 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:57:35+08:00","message":"Message msg-0 processed successfully"}
//{"level":"debug","time":"2024-07-09T16:57:35+08:00","message":"Message msg-2 processed successfully"}
