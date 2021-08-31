package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	. "github.com/sannonthachai/poc-pubsub/config"
	"github.com/subosito/gotenv"
)

func init() {
	gotenv.Load()
}

func main() {

	config := InitPubsubConfig()
	t, err := ConnectTopic(config.ProjectID, config.TopicID)
	if err != nil {
		fmt.Println(err)
	}

	start := time.Now()

	out := gen()
	createGoroutine(100, out, t, start)
}

func publish(t *pubsub.Topic, msg string) error {
	ctx := context.Background()

	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("Get: %v", err)
	}

	fmt.Printf("Published a message; msg ID: %v\n", id)
	return nil
}

func createGoroutine(numGoroutine int, in <-chan int, t *pubsub.Topic, start time.Time) {
	out := make([]<-chan string, numGoroutine)

	for i := 0; i < numGoroutine; i++ {
		out[i] = sq(in, t)
	}

	for n := range merge(out...) {
		fmt.Println(n)
		elapsed := time.Since(start)
		log.Printf("time : %s", elapsed)
	}
}

func gen() <-chan int {
	out := make(chan int)
	go func() {
		for i := 1; i < 10000; i++ {
			out <- i
		}
		close(out)
	}()
	return out
}

func sq(in <-chan int, t *pubsub.Topic) <-chan string {
	out := make(chan string)
	go func() {
		ctx := context.Background()
		for n := range in {
			result := t.Publish(ctx, &pubsub.Message{
				Data: []byte(fmt.Sprintf("%+v", n)),
			})
			id, err := result.Get(ctx)
			if err != nil {
				fmt.Println("Get : ", err)
			}

			fmt.Printf("Published a message; msg ID: %v\n", id)
			out <- id
		}

		close(out)
	}()

	return out
}

func merge(cs ...<-chan string) <-chan string {
	var wg sync.WaitGroup
	out := make(chan string)

	output := func(c <-chan string) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))

	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
