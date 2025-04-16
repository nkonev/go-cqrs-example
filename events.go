package main

import "time"

type MessageMetadata struct {
	CreatedAt time.Time
}

type SubscriberSubscribed struct {
	Metadata     *MessageMetadata `json:"metadata"`
	SubscriberId string           `json:"subscriber_id"`
	Email        string           `json:"email"`
}

type SubscriberUnsubscribed struct {
	Metadata     *MessageMetadata `json:"metadata"`
	SubscriberId string           `json:"subscriber_id"`
}

type SubscriberEmailUpdated struct {
	Metadata     *MessageMetadata `json:"metadata"`
	SubscriberId string           `json:"subscriber_id"`
	NewEmail     string           `json:"new_email"`
}

func GenerateMessageMetadata() *MessageMetadata {
	return &MessageMetadata{
		CreatedAt: time.Now().UTC(),
	}
}

func (s *SubscriberSubscribed) GetPartitionKey() string {
	return s.SubscriberId
}

func (s *SubscriberUnsubscribed) GetPartitionKey() string {
	return s.SubscriberId
}

func (s *SubscriberEmailUpdated) GetPartitionKey() string {
	return s.SubscriberId
}

func (s *SubscriberSubscribed) Name() string {
	return "subscriber_subscribed"
}

func (s *SubscriberUnsubscribed) Name() string {
	return "subscriber_unsubscribed"
}

func (s *SubscriberEmailUpdated) Name() string {
	return "subscriber_email_updated"
}
