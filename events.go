package main

import "time"

type MessageMetadata struct {
	PartitionKey string
	CreatedAt    time.Time
}

type SubscriberSubscribed struct {
	Metadata     *MessageMetadata `json:"-"`
	SubscriberId string           `json:"subscriber_id"`
	Email        string           `json:"email"`
}

type SubscriberUnsubscribed struct {
	Metadata     *MessageMetadata `json:"-"`
	SubscriberId string           `json:"subscriber_id"`
}

type SubscriberEmailUpdated struct {
	Metadata     *MessageMetadata `json:"-"`
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

func (s *SubscriberSubscribed) SetMetadata(m *MessageMetadata) {
	s.Metadata = m
}

func (s *SubscriberUnsubscribed) SetMetadata(m *MessageMetadata) {
	s.Metadata = m
}

func (s *SubscriberEmailUpdated) SetMetadata(m *MessageMetadata) {
	s.Metadata = m
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
