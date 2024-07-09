package main

import (
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"rstream/example"
	"strconv"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	if len(os.Args) < 2 {
		log.Fatalf("Please provide a test case number")
	}

	c, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid test case number: %v", err)
	}

	switch c {
	case 1:
		example.MultipleGroups(client) // 👌
	case 2:
		example.AsyncProcessing(client) // 👌
	case 3:
		example.MultipleConsumers(client) // 👌
	case 4:
		example.SingleConsumer(client) // 👌
	case 5:
		example.MessageRetry(client) // 👌
	case 6:
		example.MessageTimeout(client) // 👌
	case 7:
		example.DeadLetterQueue(client) // 👌
	default:
		log.Fatalf("Unknown test case number: %d", c)
	}

	select {}
}
