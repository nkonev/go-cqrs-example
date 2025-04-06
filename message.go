package main

import (
	"fmt"
	"log/slog"
	"strconv"
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
const CreatedAtKeyMetadataField = "created_at"

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
	msg.Metadata.Set(CreatedAtKeyMetadataField, fmt.Sprintf("%v", metadata.CreatedAt.Unix()))

	return msg, nil
}

func (c CqrsMarshalerDecorator) Unmarshal(msg *message.Message, v interface{}) (err error) {
	err = c.JSONMarshaler.Unmarshal(msg, v)
	if err != nil {
		return err
	}

	pm, ok := v.(JsonMessage)
	if !ok {
		return fmt.Errorf("%T does not implement JsonMessage and can't be unmarshaled", v)
	}

	metadata := &MessageMetadata{}

	ts := msg.Metadata.Get(CreatedAtKeyMetadataField)
	i, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return err
	}
	tm := time.Unix(i, 0).UTC()
	metadata.CreatedAt = tm
	metadata.PartitionKey = msg.Metadata.Get(PartitionKeyMetadataField)
	pm.SetMetadata(metadata)
	return nil
}

type JsonMessage interface {
	GetMetadata() *MessageMetadata
	SetMetadata(*MessageMetadata)
}

// GenerateKafkaPartitionKey is a function that generates a partition key for Kafka messages.
func GenerateKafkaPartitionKey(topic string, msg *message.Message) (string, error) {
	slog.Debug("Setting partition key", "topic", topic, "msg_metadata", msg.Metadata)

	return msg.Metadata.Get(PartitionKeyMetadataField), nil
}
