package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"log/slog"
	"time"
)

type SubscriberProjection struct {
	db *sql.DB
}

func NewSubscriberReadModel(db *sql.DB) *SubscriberProjection {
	return &SubscriberProjection{
		db: db,
	}
}

func (m *SubscriberProjection) GetSubscribers(ctx context.Context) (map[uuid.UUID]string, error) {
	ma := map[uuid.UUID]string{}
	rows, err := m.db.QueryContext(ctx, "select subscriber_id, email from subscriber order by created_timestamp")
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var u uuid.UUID
		var e string
		err = rows.Scan(&u, &e)
		if err != nil {
			return ma, err
		}
		ma[u] = e
	}
	return ma, nil
}

const NoSubscriber = ""

func (m *SubscriberProjection) GetSubscriber(ctx context.Context, subscriberId uuid.UUID) (string, error) {
	r := m.db.QueryRowContext(ctx, "select email from subscriber where subscriber_id = $1", subscriberId)
	if r.Err() != nil {
		return "", r.Err()
	}
	var e string
	err := r.Scan(&e)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return NoSubscriber, nil
	} else if err != nil {
		return "", err
	}
	return e, nil
}

func (m *SubscriberProjection) OnSubscribed(ctx context.Context, event *SubscriberSubscribed) error {
	_, err := m.db.ExecContext(ctx, "insert into subscriber(subscriber_id, email, created_timestamp) values ($1, $2, $3)", event.SubscriberId, event.Email, time.Now().UTC())
	if err != nil {
		return err
	}

	slog.Info(
		"Subscriber added",
		"subscriber_id", event.SubscriberId,
		"email", event.Email,
	)

	return nil
}

func (m *SubscriberProjection) OnUnsubscribed(ctx context.Context, event *SubscriberUnsubscribed) error {
	_, err := m.db.ExecContext(ctx, "delete from subscriber where subscriber_id = $1", event.SubscriberId)
	if err != nil {
		return err
	}

	slog.Info(
		"Subscriber removed",
		"subscriber_id", event.SubscriberId,
	)

	return nil
}

func (m *SubscriberProjection) OnEmailUpdated(ctx context.Context, event *SubscriberEmailUpdated) error {
	_, err := m.db.ExecContext(ctx, "update subscriber set email = $2 where subscriber_id = $1", event.SubscriberId, event.NewEmail)
	if err != nil {
		return err
	}

	slog.Info(
		"Subscriber updated",
		"subscriber_id", event.SubscriberId,
		"email", event.NewEmail,
	)

	return nil
}

// ActivityEntry represents a single event in the timeline
type ActivityEntry struct {
	Timestamp    time.Time `json:"timestamp"`
	SubscriberID string    `json:"subscriberId"`
	ActivityType string    `json:"activityType"`
	Details      string    `json:"details"`
}

// ActivityTimelineProjection maintains a chronological log of all subscription-related events
type ActivityTimelineProjection struct {
	db *sql.DB
}

func NewActivityTimelineModel(db *sql.DB) *ActivityTimelineProjection {
	return &ActivityTimelineProjection{
		db: db,
	}
}

// OnSubscribed handles subscription events
func (m *ActivityTimelineProjection) OnSubscribed(ctx context.Context, event *SubscriberSubscribed) error {
	entry := ActivityEntry{
		Timestamp:    time.Now().UTC(),
		SubscriberID: event.SubscriberId,
		ActivityType: "SUBSCRIBED",
		Details:      fmt.Sprintf("Subscribed with email: %s", event.Email),
	}

	_, err := m.db.ExecContext(ctx, "insert into activity_timeline(created_timestamp, subscriber_id, activity_type, details) values ($1, $2, $3, $4)", entry.Timestamp, entry.SubscriberID, entry.ActivityType, entry.Details)
	if err != nil {
		return err
	}

	m.logActivity(entry)
	return nil
}

// OnUnsubscribed handles unsubscription events
func (m *ActivityTimelineProjection) OnUnsubscribed(ctx context.Context, event *SubscriberUnsubscribed) error {
	entry := ActivityEntry{
		Timestamp:    time.Now().UTC(),
		SubscriberID: event.SubscriberId,
		ActivityType: "UNSUBSCRIBED",
		Details:      "Subscriber unsubscribed",
	}

	_, err := m.db.ExecContext(ctx, "insert into activity_timeline(created_timestamp, subscriber_id, activity_type, details) values ($1, $2, $3, $4)", entry.Timestamp, entry.SubscriberID, entry.ActivityType, entry.Details)
	if err != nil {
		return err
	}

	m.logActivity(entry)
	return nil
}

// OnEmailUpdated handles email update events
func (m *ActivityTimelineProjection) OnEmailUpdated(ctx context.Context, event *SubscriberEmailUpdated) error {

	entry := ActivityEntry{
		Timestamp:    time.Now().UTC(),
		SubscriberID: event.SubscriberId,
		ActivityType: "EMAIL_UPDATED",
		Details:      fmt.Sprintf("Email updated to: %s", event.NewEmail),
	}

	_, err := m.db.ExecContext(ctx, "insert into activity_timeline(created_timestamp, subscriber_id, activity_type, details) values ($1, $2, $3, $4)", entry.Timestamp, entry.SubscriberID, entry.ActivityType, entry.Details)
	if err != nil {
		return err
	}

	m.logActivity(entry)
	return nil
}

func (m *ActivityTimelineProjection) logActivity(entry ActivityEntry) {
	slog.Info(
		"[ACTIVITY]",
		"activity_type", entry.ActivityType,
		"subscriber_id", entry.SubscriberID,
		"details", entry.Details,
	)
}

func (m *ActivityTimelineProjection) GetActivities(ctx context.Context) ([]ActivityEntry, error) {
	res := []ActivityEntry{}
	rows, err := m.db.QueryContext(ctx, "select created_timestamp, subscriber_id, activity_type, details from activity_timeline order by created_timestamp")
	if err != nil {
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var item = ActivityEntry{}
		err = rows.Scan(&item.Timestamp, &item.SubscriberID, &item.ActivityType, &item.Details)
		if err != nil {
			return res, err
		}
		res = append(res, item)
	}
	return res, nil
}
