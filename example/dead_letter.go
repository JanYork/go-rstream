package example

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"rstream/rstream"
	"time"
)

func DeadLetterQueue(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "deadLetterQueue",
		DefaultGroup:    "deadLetterGroup",
		BlockTime:       1 * time.Second,
		FIFO:            false,
		RetryCount:      1,
		DeadLetterName:  "deadLetterDeadLetter",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10,
	}
	ctx := context.Background()

	manager := rstream.NewGroupManager(queue)
	groupKey := "dlqGroup"

	err := manager.CreateGroup(ctx, groupKey, "")
	if err != nil {
		log.Printf("Failed to create group: %v", err)
		return
	}

	go startDeadLetterConsumer(client, queue, groupKey, "dlq-consumer")
	go startDeadLetterProducer(queue, 3)
}

func startDeadLetterProducer(queue *rstream.Queue, messageCount int) {
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

func startDeadLetterConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
	processMessage := func(msg rstream.Message, tag string, ctx map[string]interface{}) error {
		log.Printf("group: %s, consumer: %s, message: %s", group, consumerName, msg.String())
		return errors.New("test error")
	}

	consumer := rstream.NewConsumer(client, queue, group, consumerName, "tag", nil, processMessage)
	ctx := context.Background()

	err := consumer.Consume(ctx)
	if err != nil {
		log.Printf("Consumer %s failed: %v", consumerName, err)
	}
}

//{"level":"debug","time":"2024-07-09T17:14:30+08:00","message":"Creating groupName dlqGroup, start: , stream: rstream:queue:deadLetterQueue"}
//{"level":"debug","time":"2024-07-09T17:14:30+08:00","message":"Published message to stream rstream:queue:deadLetterQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T17:14:30+08:00","message":"Processing stream rstream:queue:deadLetterQueue"}
//2024/07/09 17:14:30 group: dlqGroup, consumer: dlq-consumer, message: msg-0 0 2024-07-09 17:14:30.561016 +0800 CST
//{"level":"error","error":"test error","time":"2024-07-09T17:14:30+08:00","message":"Message msg-0 processed with error"}
//{"level":"debug","error":"test error","time":"2024-07-09T17:14:30+08:00","message":"Handling failure for message 1720516470563-0"}
//{"level":"debug","time":"2024-07-09T17:14:30+08:00","message":"Message 1720516470563-0 has reached max retry count"}
//{"level":"info","time":"2024-07-09T17:14:30+08:00","message":"Message 1720516470563-0 sent to dead letter stream"}
//{"level":"debug","time":"2024-07-09T17:14:31+08:00","message":"Processing stream rstream:queue:deadLetterQueue"}
//{"level":"debug","time":"2024-07-09T17:14:31+08:00","message":"Published message to stream rstream:queue:deadLetterQueue, message: msg-1"}
//2024/07/09 17:14:31 group: dlqGroup, consumer: dlq-consumer, message: msg-1 1 2024-07-09 17:14:31.564293 +0800 CST
//{"level":"error","error":"test error","time":"2024-07-09T17:14:31+08:00","message":"Message msg-1 processed with error"}
//{"level":"debug","error":"test error","time":"2024-07-09T17:14:31+08:00","message":"Handling failure for message 1720516471565-0"}
//{"level":"debug","time":"2024-07-09T17:14:31+08:00","message":"Message 1720516471565-0 has reached max retry count"}
//{"level":"info","time":"2024-07-09T17:14:31+08:00","message":"Message 1720516471565-0 sent to dead letter stream"}
//{"level":"debug","time":"2024-07-09T17:14:32+08:00","message":"Published message to stream rstream:queue:deadLetterQueue, message: msg-2"}
//{"level":"debug","time":"2024-07-09T17:14:32+08:00","message":"Processing stream rstream:queue:deadLetterQueue"}
//2024/07/09 17:14:32 group: dlqGroup, consumer: dlq-consumer, message: msg-2 2 2024-07-09 17:14:32.566639 +0800 CST
//{"level":"error","error":"test error","time":"2024-07-09T17:14:32+08:00","message":"Message msg-2 processed with error"}
//{"level":"debug","error":"test error","time":"2024-07-09T17:14:32+08:00","message":"Handling failure for message 1720516472567-0"}
//{"level":"debug","time":"2024-07-09T17:14:32+08:00","message":"Message 1720516472567-0 has reached max retry count"}
//{"level":"info","time":"2024-07-09T17:14:32+08:00","message":"Message 1720516472567-0 sent to dead letter stream"}
