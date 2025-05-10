package cqrs

import (
	"errors"
	"fmt"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

type CqrsMarshalerDecorator struct {
	cqrs.JSONMarshaler
}

func (c CqrsMarshalerDecorator) Unmarshal(msg *message.Message, v interface{}) (err error) {
	err = c.JSONMarshaler.Unmarshal(msg, v)
	if err != nil {
		return err
	}

	pm, ok := v.(OffsetableMessage)
	if !ok {
		return fmt.Errorf("%T does not implement OffsetableMessage and can't be unmarshaled", v)
	}

	partition, ok := kafka.MessagePartitionFromCtx(msg.Context())
	if !ok {
		return errors.New("Unable to get partition")
	}

	offset, ok := kafka.MessagePartitionOffsetFromCtx(msg.Context())
	if !ok {
		return errors.New("Unable to get offset")
	}

	pm.SetOffset(partition, offset)

	return nil
}

type OffsetableMessage interface {
	SetOffset(int32, int64)
}
