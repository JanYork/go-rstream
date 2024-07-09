package example

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"rstream/rstream"
	"time"
)

func MessageRetry(client *redis.Client) {
	queue := &rstream.Queue{
		Client:          client,
		Name:            "messageRetryQueue",
		DefaultGroup:    "messageRetryGroup",
		BlockTime:       1 * time.Second,
		FIFO:            false,
		RetryCount:      3,
		DeadLetterName:  "messageRetryDeadLetter",
		LongPollingTime: 2 * time.Second,
		MaxLength:       1000,
		MaxConsumers:    5,
		MaxGroups:       3,
		MaxConcurrency:  10,
	}

	ctx := context.Background()

	manager := rstream.NewGroupManager(queue)
	groupKey := "retryGroup"

	err := manager.CreateGroup(ctx, groupKey, "")
	if err != nil {
		log.Printf("Failed to create group: %v", err)
		return
	}

	go startRetryConsumer(client, queue, groupKey, "retry-consumer")
	startRetryProducer(queue, 1)
}

func startRetryProducer(queue *rstream.Queue, messageCount int) {
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

func startRetryConsumer(client *redis.Client, queue *rstream.Queue, group, consumerName string) {
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

//{"level":"debug","time":"2024-07-09T17:02:55+08:00","message":"Creating groupName retryGroup, start: , stream: rstream:queue:messageRetryQueue"}
//{"level":"debug","time":"2024-07-09T17:02:55+08:00","message":"Published message to stream rstream:queue:messageRetryQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T17:02:55+08:00","message":"Processing stream rstream:queue:messageRetryQueue"}
//2024/07/09 17:02:55 group: retryGroup, consumer: retry-consumer, message: msg-0 0 2024-07-09 17:02:55.724675 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:02:58+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:02:58+08:00","message":"Handling failure for message 1720515775724-0"}
//{"level":"debug","time":"2024-07-09T17:02:58+08:00","message":"Retrying message 1720515775724-0"}
//{"level":"debug","time":"2024-07-09T17:02:58+08:00","message":"Message 1720515775724-0 reassigned successfully"}
//2024/07/09 17:02:58 group: retryGroup, consumer: retry-consumer, message: msg-0 0 2024-07-09 17:02:55.724675 +0800 CST
//{"level":"debug","time":"2024-07-09T17:03:01+08:00","message":"Message msg-0 processed successfully"}

// --- 上面为恰好超时测试，下面为必然超时测试 ---

//{"level":"debug","time":"2024-07-09T17:11:57+08:00","message":"Creating groupName retryGroup, start: , stream: rstream:queue:messageRetryQueue"}
//{"level":"debug","time":"2024-07-09T17:11:57+08:00","message":"Published message to stream rstream:queue:messageRetryQueue, message: msg-0"}
//{"level":"debug","time":"2024-07-09T17:11:57+08:00","message":"Processing stream rstream:queue:messageRetryQueue"}
//2024/07/09 17:11:57 group: retryGroup, consumer: retry-consumer, message: msg-0 0 2024-07-09 17:11:57.626811 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:11:58+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:11:58+08:00","message":"Handling failure for message 1720516317626-0"}
//{"level":"debug","time":"2024-07-09T17:11:58+08:00","message":"Retrying message 1720516317626-0"}
//{"level":"debug","time":"2024-07-09T17:11:58+08:00","message":"Message 1720516317626-0 reassigned successfully"}
//2024/07/09 17:11:58 group: retryGroup, consumer: retry-consumer, message: msg-0 0 2024-07-09 17:11:57.626811 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:11:59+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:11:59+08:00","message":"Handling failure for message 1720516317626-0"}
//{"level":"debug","time":"2024-07-09T17:11:59+08:00","message":"Retrying message 1720516317626-0"}
//{"level":"debug","time":"2024-07-09T17:11:59+08:00","message":"Message 1720516317626-0 reassigned successfully"}
//2024/07/09 17:11:59 group: retryGroup, consumer: retry-consumer, message: msg-0 0 2024-07-09 17:11:57.626811 +0800 CST
//{"level":"error","error":"context deadline exceeded","time":"2024-07-09T17:12:00+08:00","message":"Message msg-0 processing timed out"}
//{"level":"debug","error":"context deadline exceeded","time":"2024-07-09T17:12:00+08:00","message":"Handling failure for message 1720516317626-0"}
//{"level":"debug","time":"2024-07-09T17:12:00+08:00","message":"Message 1720516317626-0 has reached max retry count"}
//{"level":"info","time":"2024-07-09T17:12:00+08:00","message":"Message 1720516317626-0 sent to dead letter stream"}
