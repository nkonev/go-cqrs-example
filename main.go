package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"log/slog"
	"net/http"
	"time"
)

func main() {

	kafkaBootstrapServers := []string{"127.0.0.1:9092"}

	slog.SetLogLoggerLevel(slog.LevelDebug)

	logger := watermill.NewSlogLoggerWithLevelMapping(nil, map[slog.Level]slog.Level{
		slog.LevelInfo: slog.LevelDebug,
	})

	// We are decorating ProtobufMarshaler to add extra metadata to the message.
	cqrsMarshaler := CqrsMarshalerDecorator{
		cqrs.JSONMarshaler{
			// It will generate topic names based on the event/command type.
			// So for example, for "RoomBooked" name will be "RoomBooked".
			GenerateName: cqrs.NamedStruct(func(v interface{}) string {
				panic(fmt.Sprintf("not implemented Name() for %T", v))
			}),
		},
	}

	watermillLogger := watermill.NewSlogLoggerWithLevelMapping(
		slog.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)

	// This marshaler converts Watermill messages to Kafka messages.
	// We are using it to add partition key to the Kafka message.
	kafkaMarshaler := kafka.NewWithPartitioningMarshaler(GenerateKafkaPartitionKey)

	// You can use any Pub/Sub implementation from here: https://watermill.io/pubsubs/
	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   kafkaBootstrapServers,
			Marshaler: kafkaMarshaler,
		},
		watermillLogger,
	)
	if err != nil {
		panic(err)
	}

	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	cqrsRouter, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	cqrsRouter.AddMiddleware(middleware.Recoverer)
	cqrsRouter.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			slog.Debug("Received message", "metadata", msg.Metadata)
			return h(msg)
		}
	})

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			// We are using one topic for all events to maintain the order of events.
			return "events", nil
		},
		Marshaler: cqrsMarshaler,
		Logger:    logger,
	})
	if err != nil {
		panic(err)
	}

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		cqrsRouter,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return "events", nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						Brokers:       kafkaBootstrapServers,
						ConsumerGroup: params.EventGroupName,
						Unmarshaler:   kafkaMarshaler,
					},
					watermillLogger,
				)
			},
			Marshaler: cqrsMarshaler,
			Logger:    logger,
		},
	)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("pgx", "postgres://postgres:postgresqlPassword@localhost:5432/postgres?sslmode=disable&application_name=cqrs-app")
	if err != nil {
		panic(err)
	}
	db.SetConnMaxLifetime(time.Second * 30)
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(16)
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	subscriberProjection := NewSubscriberProjection(db)

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		"SubscriberProjection",
		cqrs.NewGroupEventHandler(subscriberProjection.OnSubscribed),
		cqrs.NewGroupEventHandler(subscriberProjection.OnUnsubscribed),
		cqrs.NewGroupEventHandler(subscriberProjection.OnEmailUpdated),
	)
	if err != nil {
		panic(err)
	}

	activityProjection := NewActivityTimelineProjection(db)

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		"ActivityTimelineProjection",
		cqrs.NewGroupEventHandler(activityProjection.OnSubscribed),
		cqrs.NewGroupEventHandler(activityProjection.OnUnsubscribed),
		cqrs.NewGroupEventHandler(activityProjection.OnEmailUpdated),
	)
	if err != nil {
		panic(err)
	}

	slog.Info("Starting service")

	httpRouter := http.NewServeMux()
	httpRouter.HandleFunc("POST /subscribe", func(w http.ResponseWriter, r *http.Request) {
		subscriberID := watermill.NewUUID()

		c := Subscribe{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
		}

		err := c.Handle(r.Context(), eventBus, subscriberProjection)

		if err != nil {
			slog.Error("Error sending Subscribe command", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		m := map[string]string{
			"id": subscriberID,
		}
		b, err := json.Marshal(m)
		if err != nil {
			slog.Error("Error marshalling response", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})

	httpRouter.HandleFunc("PUT /update/{id}", func(w http.ResponseWriter, r *http.Request) {
		subscriberID := r.PathValue("id")

		c := UpdateEmail{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
			NewEmail:     fmt.Sprintf("updated%d@example.com", time.Now().UTC().Unix()),
		}
		err := c.Handle(r.Context(), eventBus, subscriberProjection)
		if err != nil {
			slog.Error("Error sending UpdateEmail command", "err", err)
			w.WriteHeader(http.StatusInternalServerError)

			m := map[string]string{
				"msg": err.Error(),
			}
			b, err := json.Marshal(m)
			if err != nil {
				slog.Error("Error marshalling response", "err", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			w.Write(b)
			return
		}
	})

	httpRouter.HandleFunc("POST /unsubscribe/{id}", func(w http.ResponseWriter, r *http.Request) {
		subscriberID := r.PathValue("id")

		c := Unsubscribe{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
		}
		err := c.Handle(r.Context(), eventBus, subscriberProjection)
		if err != nil {
			slog.Error("Error sending Unsubscribe command", "err", err)
			w.WriteHeader(http.StatusInternalServerError)

			m := map[string]string{
				"msg": err.Error(),
			}
			b, err := json.Marshal(m)
			if err != nil {
				slog.Error("Error marshalling response", "err", err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			w.Write(b)
			return
		}
	})

	httpRouter.HandleFunc("GET /subscribers", func(w http.ResponseWriter, r *http.Request) {
		subscribers, err := subscriberProjection.GetSubscribers(r.Context())
		if err != nil {
			slog.Error("Error getting subscribers", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		b, err := json.Marshal(subscribers)
		if err != nil {
			slog.Error("Error marshalling", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})

	httpRouter.HandleFunc("GET /activities", func(w http.ResponseWriter, r *http.Request) {
		activities, err := activityProjection.GetActivities(r.Context())
		if err != nil {
			slog.Error("Error getting activities", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		b, err := json.Marshal(activities)
		if err != nil {
			slog.Error("Error marshalling", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(b)
	})

	go http.ListenAndServe(":8080", httpRouter)

	if err := cqrsRouter.Run(context.Background()); err != nil {
		panic(err)
	}
}
