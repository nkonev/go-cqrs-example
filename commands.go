package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
)

type Subscribe struct {
	Metadata     *MessageMetadata
	SubscriberId string
}

type Unsubscribe struct {
	Metadata     *MessageMetadata
	SubscriberId string
}

type UpdateEmail struct {
	Metadata     *MessageMetadata
	SubscriberId string
	NewEmail     string
}

func (s *Subscribe) Handle(ctx context.Context, eventBus EventBusInterface, subscriberProjection *SubscriberProjection) error {
	emailId, err := subscriberProjection.GetNextEmailId(ctx)
	if err != nil {
		return err
	}

	email := fmt.Sprintf("user%d@example.com", emailId)

	e := &SubscriberSubscribed{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
		Email:        email,
	}

	return eventBus.Publish(ctx, e)
}

func (s *Unsubscribe) Handle(ctx context.Context, eventBus EventBusInterface, subscriberProjection *SubscriberProjection) error {
	// here is logic with business rules validation
	subscriber, err := subscriberProjection.GetSubscriber(ctx, uuid.MustParse(s.SubscriberId))
	if err != nil {
		return err
	}
	if subscriber == NoSubscriber {
		return fmt.Errorf("Subscriber with id = %v isn't found", s.SubscriberId)
	}

	e := &SubscriberUnsubscribed{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
	}

	return eventBus.Publish(ctx, e)
}

func (s *UpdateEmail) Handle(ctx context.Context, eventBus EventBusInterface, subscriberProjection *SubscriberProjection) error {
	// here is logic with business rules validation
	subscriber, err := subscriberProjection.GetSubscriber(ctx, uuid.MustParse(s.SubscriberId))
	if err != nil {
		return err
	}
	if subscriber == NoSubscriber {
		return fmt.Errorf("Subscriber with id = %v isn't found", s.SubscriberId)
	}

	e := &SubscriberEmailUpdated{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
		NewEmail:     s.NewEmail,
	}

	return eventBus.Publish(ctx, e)
}
