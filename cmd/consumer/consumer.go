package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	. "github.com/sannonthachai/poc-pubsub/config"
	"github.com/subosito/gotenv"
)

type Message struct {
	Messages *pubsub.Message
	Err      error
}

func (e *Message) Error() string {
	return e.Err.Error()
}

func init() {
	gotenv.Load()
}

func main() {
	config := InitPubsubConfig()
	s, err := ConnectSubscription(config.ProjectID, config.SubscriptionID)
	if err != nil {
		fmt.Println(err)
	}

	// s.ReceiveSettings.Synchronous = true
	// s.ReceiveSettings.MaxOutstandingMessages = 5000
	// s.ReceiveSettings.NumGoroutines = 100

	c1 := pullMsgs(s)
	createGoroutine(100, c1)
}

func pullMsgs(sub *pubsub.Subscription) <-chan *pubsub.Message {
	out := make(chan *pubsub.Message)
	ctx := context.Background()

	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			out <- msg
		})
		if err != nil {
			fmt.Println("Receive error", err)
		}
		close(out)
	}()

	return out
}

func fanout(in <-chan *pubsub.Message) <-chan Message {
	out := make(chan Message)
	messages := Message{}
	go func() {
		for msg := range in {
			time.Sleep(time.Millisecond * 500)
			// dap := DapCallbackPayload{}
			// json.Unmarshal(msg.Data, &dap)
			// fmt.Printf("Got message :%v\n", string(msg.Data))
			// fmt.Println("Attributes:")
			// for key, value := range msg.Attributes {
			// fmt.Printf("%s = %s", key, value)
			// }
			messages.Messages = msg
			out <- messages
		}
		close(out)
	}()

	return out
}

func createGoroutine(numGoroutine int, in <-chan *pubsub.Message) {
	out := make([]<-chan Message, numGoroutine)
	start := time.Now()

	for i := 0; i < numGoroutine; i++ {
		out[i] = fanout(in)
	}

	for msg := range merge(out...) {
		if msg.Err != nil {
			fmt.Println("Error : ", msg.Error())
			continue
		}

		msg.Messages.Ack()
		elapsed := time.Since(start)
		fmt.Println(string(msg.Messages.Data))
		fmt.Println(fmt.Sprintf("time : %s", elapsed))
	}
}

func merge(cs ...<-chan Message) <-chan Message {
	var wg sync.WaitGroup
	out := make(chan Message)

	output := func(c <-chan Message) {
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
