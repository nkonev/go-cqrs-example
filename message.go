package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

func GenerateMessageMetadata(partitionKey string) *MessageMetadata {
	return &MessageMetadata{
		PartitionKey: partitionKey,
		CreatedAt:    time.Now().UTC(),
	}
}

type CqrsMarshalerDecorator struct {
	cqrs.JSONMarshaler
}

const PartitionKeyMetadataField = "partition_key"

func (c CqrsMarshalerDecorator) Marshal(v interface{}) (*message.Message, error) {
	msg, err := c.JSONMarshaler.Marshal(v)
	if err != nil {
		return nil, err
	}

	pm, ok := v.(JsonMessage)
	if !ok {
		return nil, fmt.Errorf("%T does not implement JsonMessage and can't be marshaled", v)
	}

	metadata := pm.GetMetadata()
	if metadata == nil {
		return nil, fmt.Errorf("%T.GetMetadata returned nil", v)
	}

	msg.Metadata.Set(PartitionKeyMetadataField, metadata.PartitionKey)
	msg.Metadata.Set("created_at", metadata.CreatedAt.String())

	return msg, nil
}

type JsonMessage interface {
	GetMetadata() *MessageMetadata
}

// GenerateKafkaPartitionKey is a function that generates a partition key for Kafka messages.
func GenerateKafkaPartitionKey(topic string, msg *message.Message) (string, error) {
	slog.Debug("Setting partition key", "topic", topic, "msg_metadata", msg.Metadata)

	return msg.Metadata.Get(PartitionKeyMetadataField), nil
}
