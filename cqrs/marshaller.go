package cqrs

import (
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

	return nil
}
