package cqrs

import (
	"context"
	"errors"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"log/slog"
	"main.go/logger"
)

const partitionKey = "partition_key"

type EventBusInterface interface {
	Publish(ctx context.Context, event PartitionableMessage) error
}

type PartitionAwareEventBus struct {
	eventBus *cqrs.EventBus
}

func (w *PartitionAwareEventBus) Publish(ctx context.Context, pm PartitionableMessage) error {
	// we put partition key into context in order tot to duplicate partition key, stored in kafka key into headers
	return w.eventBus.Publish(makeContextWithPartitionKey(ctx, pm), pm)
}

type PartitionableMessage interface {
	GetPartitionKey() string
}

// GenerateKafkaPartitionKey is a function that generates a partition key for Kafka messages.
func GenerateKafkaPartitionKey(slogLogger *slog.Logger) kafka.GeneratePartitionKey {
	return func(topic string, msg *message.Message) (string, error) {
		pk, ok := msg.Context().Value(partitionKey).(string)
		if !ok {
			return "", errors.New("unable to get partition key from context")
		}
		logger.LogWithTrace(msg.Context(), slogLogger).Debug("retrieving partition key", "topic", topic, "msg_metadata", msg.Metadata, partitionKey, pk)
		return pk, nil
	}
}

func makeContextWithPartitionKey(parent context.Context, pm PartitionableMessage) context.Context {
	pk := pm.GetPartitionKey()
	return context.WithValue(parent, partitionKey, pk)
}
