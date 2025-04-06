package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
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

func (s *Subscribe) Handle(ctx context.Context, eventBus *cqrs.EventBus, subscriberProjection *SubscriberProjection) error {
	emailId, err := subscriberProjection.GetNextEmailId(ctx)
	if err != nil {
		return err
	}

	email := fmt.Sprintf("user%d@example.com", emailId)

	return eventBus.Publish(ctx, &SubscriberSubscribed{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
		Email:        email,
	})
}

func (s *Unsubscribe) Handle(ctx context.Context, eventBus *cqrs.EventBus, subscriberProjection *SubscriberProjection) error {
	// here is logic with business rules validation
	subscriber, err := subscriberProjection.GetSubscriber(ctx, uuid.MustParse(s.SubscriberId))
	if err != nil {
		return err
	}
	if subscriber == NoSubscriber {
		return fmt.Errorf("Subscriber with id = %v isn't found", s.SubscriberId)
	}

	return eventBus.Publish(ctx, &SubscriberUnsubscribed{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
	})
}

func (s *UpdateEmail) Handle(ctx context.Context, eventBus *cqrs.EventBus, subscriberProjection *SubscriberProjection) error {
	// here is logic with business rules validation
	subscriber, err := subscriberProjection.GetSubscriber(ctx, uuid.MustParse(s.SubscriberId))
	if err != nil {
		return err
	}
	if subscriber == NoSubscriber {
		return fmt.Errorf("Subscriber with id = %v isn't found", s.SubscriberId)
	}

	return eventBus.Publish(ctx, &SubscriberEmailUpdated{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
		NewEmail:     s.NewEmail,
	})
}
