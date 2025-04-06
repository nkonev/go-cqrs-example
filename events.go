package main

import "time"

type MessageMetadata struct {
	PartitionKey string    `json:"partition_key"`
	CreatedAt    time.Time `json:"created_at"`
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

func (s *SubscriberSubscribed) GetMetadata() *MessageMetadata {
	return s.Metadata
}

func (s *SubscriberUnsubscribed) GetMetadata() *MessageMetadata {
	return s.Metadata
}

func (s *SubscriberEmailUpdated) GetMetadata() *MessageMetadata {
	return s.Metadata
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
