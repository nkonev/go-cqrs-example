package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type Subscribe struct {
	Metadata     *MessageMetadata `json:"metadata"`
	SubscriberId string           `json:"subscriber_id"`
	Email        string           `json:"omitempty"`
}

type Unsubscribe struct {
	Metadata     *MessageMetadata `json:"metadata"`
	SubscriberId string           `json:"subscriber_id"`
}

type UpdateEmail struct {
	Metadata     *MessageMetadata `json:"metadata"`
	SubscriberId string           `json:"subscriber_id"`
	NewEmail     string           `json:"new_email"`
}

func (s *Subscribe) Handle(ctx context.Context, eventBus *cqrs.EventBus) error {
	return eventBus.Publish(ctx, &SubscriberSubscribed{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
		Email:        s.Email,
	})
}

func (s *Unsubscribe) Handle(ctx context.Context, eventBus *cqrs.EventBus, subscribersReadModel *SubscriberReadModel) error {
	subscribersReadModel.lock.RLock()
	defer subscribersReadModel.lock.RUnlock()
	// here is logic with business rules validation
	_, ok := subscribersReadModel.subscribers[s.SubscriberId]
	if !ok {
		return fmt.Errorf("Subscriber with id = %v isn't found", s.SubscriberId)
	}

	return eventBus.Publish(ctx, &SubscriberUnsubscribed{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
	})
}

func (s *UpdateEmail) Handle(ctx context.Context, eventBus *cqrs.EventBus, subscribersReadModel *SubscriberReadModel) error {
	subscribersReadModel.lock.RLock()
	defer subscribersReadModel.lock.RUnlock()
	// here is logic with business rules validation
	_, ok := subscribersReadModel.subscribers[s.SubscriberId]
	if !ok {
		return fmt.Errorf("Subscriber with id = %v isn't found", s.SubscriberId)
	}

	return eventBus.Publish(ctx, &SubscriberEmailUpdated{
		Metadata:     s.Metadata,
		SubscriberId: s.SubscriberId,
		NewEmail:     s.NewEmail,
	})
}
