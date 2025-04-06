package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type SubscriberReadModel struct {
	subscribers map[string]string // map[subscriberID]email
	lock        sync.RWMutex
}

func NewSubscriberReadModel() *SubscriberReadModel {
	return &SubscriberReadModel{
		subscribers: make(map[string]string),
	}
}

func (m *SubscriberReadModel) OnSubscribed(ctx context.Context, event *SubscriberSubscribed) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscribers[event.SubscriberId] = event.Email

	slog.Info(
		"Subscriber added",
		"subscriber_id", event.SubscriberId,
		"email", event.Email,
	)

	return nil
}

func (m *SubscriberReadModel) OnUnsubscribed(ctx context.Context, event *SubscriberUnsubscribed) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.subscribers, event.SubscriberId)

	slog.Info(
		"Subscriber removed",
		"subscriber_id", event.SubscriberId,
	)

	return nil
}

func (m *SubscriberReadModel) OnEmailUpdated(ctx context.Context, event *SubscriberEmailUpdated) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.subscribers[event.SubscriberId] = event.NewEmail

	slog.Info(
		"Subscriber updated",
		"subscriber_id", event.SubscriberId,
		"email", event.NewEmail,
	)

	return nil
}

func (m *SubscriberReadModel) GetSubscriberCount() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.subscribers)
}

// ActivityEntry represents a single event in the timeline
type ActivityEntry struct {
	Timestamp    time.Time `json:"timestamp"`
	SubscriberID string    `json:"subscriberId"`
	ActivityType string    `json:"activityType"`
	Details      string    `json:"details"`
}

// ActivityTimelineReadModel maintains a chronological log of all subscription-related events
type ActivityTimelineReadModel struct {
	activities []ActivityEntry
	lock       sync.RWMutex
}

func NewActivityTimelineModel() *ActivityTimelineReadModel {
	return &ActivityTimelineReadModel{
		activities: make([]ActivityEntry, 0),
	}
}

// OnSubscribed handles subscription events
func (m *ActivityTimelineReadModel) OnSubscribed(ctx context.Context, event *SubscriberSubscribed) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry := ActivityEntry{
		Timestamp:    time.Now(),
		SubscriberID: event.SubscriberId,
		ActivityType: "SUBSCRIBED",
		Details:      fmt.Sprintf("Subscribed with email: %s", event.Email),
	}

	m.activities = append(m.activities, entry)
	m.logActivity(entry)
	return nil
}

// OnUnsubscribed handles unsubscription events
func (m *ActivityTimelineReadModel) OnUnsubscribed(ctx context.Context, event *SubscriberUnsubscribed) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry := ActivityEntry{
		Timestamp:    time.Now(),
		SubscriberID: event.SubscriberId,
		ActivityType: "UNSUBSCRIBED",
		Details:      "Subscriber unsubscribed",
	}

	m.activities = append(m.activities, entry)
	m.logActivity(entry)
	return nil
}

// OnEmailUpdated handles email update events
func (m *ActivityTimelineReadModel) OnEmailUpdated(ctx context.Context, event *SubscriberEmailUpdated) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	entry := ActivityEntry{
		Timestamp:    time.Now(),
		SubscriberID: event.SubscriberId,
		ActivityType: "EMAIL_UPDATED",
		Details:      fmt.Sprintf("Email updated to: %s", event.NewEmail),
	}

	m.activities = append(m.activities, entry)
	m.logActivity(entry)
	return nil
}

func (m *ActivityTimelineReadModel) logActivity(entry ActivityEntry) {
	slog.Info(
		"[ACTIVITY]",
		"activity_type", entry.ActivityType,
		"subscriber_id", entry.SubscriberID,
		"details", entry.Details,
	)
}
