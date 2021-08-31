package config

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
)

type Pubsub struct {
	ProjectID      string
	TopicID        string
	SubscriptionID string
}

func ConnectSubscription(projectID, subID string) (*pubsub.Subscription, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %v", err)
	}

	s := client.Subscription(subID)
	return s, nil

}

func ConnectTopic(projectID, topicID string) (*pubsub.Topic, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %v", err)
	}

	t := client.Topic(topicID)
	return t, nil
}

func InitPubsubConfig() Pubsub {
	return Pubsub{
		ProjectID:      os.Getenv("PROJECT_ID"),
		TopicID:        os.Getenv("TOPIC_ID"),
		SubscriptionID: os.Getenv("SUB_ID"),
	}
}

func InitPubsubConfig2() Pubsub {
	return Pubsub{
		ProjectID:      os.Getenv("PROJECT_ID"),
		TopicID:        os.Getenv("TOPIC_ID_2"),
		SubscriptionID: os.Getenv("SUB_ID_2"),
	}
}
