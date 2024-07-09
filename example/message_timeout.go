package example

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"rstream/rstream"
	"time"
)

func MessageTimeout(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "messageTimeoutQueue",
		DefaultGroup:    "messageTimeoutGroup",
		BlockTime:       1 * time.Second,
		FIFO:            true,
		RetryCount:      3,
		DeadLetterName:  "messageTimeoutDeadLetter",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10, // FIFO 开启后，MaxConcurrency 会被忽略
	}

	ctx := context.Background()

	manager := rstream.NewGroupManager(queue)
	groupKey := "timeoutGroup"

	err := manager.CreateGroup(ctx, groupKey, "")
	if err != nil {
		log.Printf("Failed to create group: %v", err)
		return
	}

	go startTimeoutConsumer(client, queue, groupKey, "timeout-consumer")
	startTimeoutProducer(queue, 3)
}

func startTimeoutProducer(queue *rstream.Queue, messageCount int) {
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

func startTimeoutConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
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

//{"level":"debug","time":"2024-07-09T17:10:41+08:00","message":"Creating groupName timeoutGroup, start: , stream: rstream:queue:messageTimeoutQueue"}
//{"level":"debug","time":"2024-07-09T17:10:41+08:00","message":"Published message to stream rstream:queue:messageTimeoutQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T17:10:41+08:00","message":"Processing stream rstream:queue:messageTimeoutQueue"}
//2024/07/09 17:10:41 group: timeoutGroup, consumer: timeout-consumer, message: msg-0 0 2024-07-09 17:10:41.991474 +0800 CST
//{"level":"debug","time":"2024-07-09T17:10:41+08:00","message":"FIFO queue has active workers"}
//{"level":"debug","time":"2024-07-09T17:10:42+08:00","message":"FIFO queue has active workers"}
//{"level":"debug","time":"2024-07-09T17:10:42+08:00","message":"FIFO queue has active workers"}
//{"level":"debug","time":"2024-07-09T17:10:42+08:00","message":"Published message to stream rstream:queue:messageTimeoutQueue, message: msg-1"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:42+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:42+08:00","message":"Handling failure for message 1720516241991-0"}
//{"level":"debug","time":"2024-07-09T17:10:42+08:00","message":"Retrying message 1720516241991-0"}
//{"level":"debug","time":"2024-07-09T17:10:42+08:00","message":"Message 1720516241991-0 reassigned successfully"}
//2024/07/09 17:10:42 group: timeoutGroup, consumer: timeout-consumer, message: msg-0 0 2024-07-09 17:10:41.991474 +0800 CST
//{"level":"debug","time":"2024-07-09T17:10:43+08:00","message":"FIFO queue has active workers"}
//{"level":"debug","time":"2024-07-09T17:10:43+08:00","message":"Published message to stream rstream:queue:messageTimeoutQueue, message: msg-2"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:43+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:43+08:00","message":"Handling failure for message 1720516241991-0"}
//{"level":"debug","time":"2024-07-09T17:10:43+08:00","message":"Retrying message 1720516241991-0"}
//{"level":"debug","time":"2024-07-09T17:10:43+08:00","message":"Message 1720516241991-0 reassigned successfully"}
//2024/07/09 17:10:43 group: timeoutGroup, consumer: timeout-consumer, message: msg-0 0 2024-07-09 17:10:41.991474 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:44+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:44+08:00","message":"Handling failure for message 1720516241991-0"}
//{"level":"debug","time":"2024-07-09T17:10:44+08:00","message":"Message 1720516241991-0 has reached max retry count"}
//{"level":"debug","time":"2024-07-09T17:10:44+08:00","message":"FIFO queue has active workers"}
//{"level":"info","time":"2024-07-09T17:10:44+08:00","message":"Message 1720516241991-0 sent to dead letter stream"}
//{"level":"debug","time":"2024-07-09T17:10:46+08:00","message":"Processing stream rstream:queue:messageTimeoutQueue"}
//2024/07/09 17:10:46 group: timeoutGroup, consumer: timeout-consumer, message: msg-1 1 2024-07-09 17:10:42.992701 +0800 CST
//{"level":"debug","time":"2024-07-09T17:10:46+08:00","message":"FIFO queue has active workers"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:47+08:00","message":"Message msg-1 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:47+08:00","message":"Handling failure for message 1720516242992-0"}
//{"level":"debug","time":"2024-07-09T17:10:47+08:00","message":"Retrying message 1720516242992-0"}
//{"level":"debug","time":"2024-07-09T17:10:47+08:00","message":"Message 1720516242992-0 reassigned successfully"}
//2024/07/09 17:10:47 group: timeoutGroup, consumer: timeout-consumer, message: msg-1 1 2024-07-09 17:10:42.992701 +0800 CST
//{"level":"debug","time":"2024-07-09T17:10:48+08:00","message":"FIFO queue has active workers"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:48+08:00","message":"Message msg-1 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:48+08:00","message":"Handling failure for message 1720516242992-0"}
//{"level":"debug","time":"2024-07-09T17:10:48+08:00","message":"Retrying message 1720516242992-0"}
//{"level":"debug","time":"2024-07-09T17:10:48+08:00","message":"Message 1720516242992-0 reassigned successfully"}
//2024/07/09 17:10:48 group: timeoutGroup, consumer: timeout-consumer, message: msg-1 1 2024-07-09 17:10:42.992701 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:49+08:00","message":"Message msg-1 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:49+08:00","message":"Handling failure for message 1720516242992-0"}
//{"level":"debug","time":"2024-07-09T17:10:49+08:00","message":"Message 1720516242992-0 has reached max retry count"}
//{"level":"info","time":"2024-07-09T17:10:49+08:00","message":"Message 1720516242992-0 sent to dead letter stream"}
//{"level":"debug","time":"2024-07-09T17:10:50+08:00","message":"Processing stream rstream:queue:messageTimeoutQueue"}
//2024/07/09 17:10:50 group: timeoutGroup, consumer: timeout-consumer, message: msg-2 2 2024-07-09 17:10:43.994507 +0800 CST
//{"level":"debug","time":"2024-07-09T17:10:50+08:00","message":"FIFO queue has active workers"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:51+08:00","message":"Message msg-2 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:51+08:00","message":"Handling failure for message 1720516243994-0"}
//{"level":"debug","time":"2024-07-09T17:10:51+08:00","message":"Retrying message 1720516243994-0"}
//{"level":"debug","time":"2024-07-09T17:10:51+08:00","message":"Message 1720516243994-0 reassigned successfully"}
//2024/07/09 17:10:51 group: timeoutGroup, consumer: timeout-consumer, message: msg-2 2 2024-07-09 17:10:43.994507 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:52+08:00","message":"Message msg-2 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:52+08:00","message":"Handling failure for message 1720516243994-0"}
//{"level":"debug","time":"2024-07-09T17:10:52+08:00","message":"Retrying message 1720516243994-0"}
//{"level":"debug","time":"2024-07-09T17:10:52+08:00","message":"Message 1720516243994-0 reassigned successfully"}
//2024/07/09 17:10:52 group: timeoutGroup, consumer: timeout-consumer, message: msg-2 2 2024-07-09 17:10:43.994507 +0800 CST
//{"level":"debug","time":"2024-07-09T17:10:52+08:00","message":"FIFO queue has active workers"}
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:10:53+08:00","message":"Message msg-2 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:10:53+08:00","message":"Handling failure for message 1720516243994-0"}
//{"level":"debug","time":"2024-07-09T17:10:53+08:00","message":"Message 1720516243994-0 has reached max retry count"}
//{"level":"info","time":"2024-07-09T17:10:53+08:00","message":"Message 1720516243994-0 sent to dead letter stream"}
