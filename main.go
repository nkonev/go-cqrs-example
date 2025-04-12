package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/gin-gonic/gin"
	sloggin "github.com/samber/slog-gin"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// TODO add traceId into Kafka headers
// TODO think about sequence restoration - introduce an new event
func main() {
	kafkaBootstrapServers := []string{"127.0.0.1:9092"}
	topicName := "events"

	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	watermillLoggerAdapter := watermill.NewSlogLoggerWithLevelMapping(slogLogger, map[slog.Level]slog.Level{
		slog.LevelInfo: slog.LevelDebug,
	})

	kafkaAdminConfig := sarama.NewConfig()
	kafkaAdminConfig.Version = sarama.V4_0_0_0

	kafkaAdmin, err := sarama.NewClusterAdmin(kafkaBootstrapServers, kafkaAdminConfig)
	if err != nil {
		panic(err)
	}

	retention := "-1"
	err = kafkaAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: map[string]*string{
			// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
			"retention.ms": &retention,
		},
	}, false)
	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
		slogLogger.Info("Topic is already exists", "topic", topicName)
	} else if err != nil {
		panic(err)
	} else {
		slogLogger.Info("Topic was successfully created", "topic", topicName)
	}
	err = kafkaAdmin.Close()
	if err != nil {
		slogLogger.Error("Error during closing kafka admin", "err", err)
	}

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
		slogLogger.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)

	// This marshaler converts Watermill messages to Kafka messages.
	// We are using it to add partition key to the Kafka message.
	kafkaMarshaler := kafka.NewWithPartitioningMarshaler(GenerateKafkaPartitionKey)

	// You can use any Pub/Sub implementation from here: https://watermill.io/pubsubs/
	kafkaProducerConfig := sarama.NewConfig()
	kafkaProducerConfig.Producer.Retry.Max = 10
	kafkaProducerConfig.Producer.Return.Successes = true
	kafkaProducerConfig.Version = sarama.V4_0_0_0
	kafkaProducerConfig.Metadata.Retry.Backoff = time.Second * 2
	kafkaProducerConfig.ClientID = "go-cqrs-producer"

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               kafkaBootstrapServers,
			OverwriteSaramaConfig: kafkaProducerConfig,
			Marshaler:             kafkaMarshaler,
		},
		watermillLogger,
	)
	if err != nil {
		panic(err)
	}

	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	cqrsRouter, err := message.NewRouter(message.RouterConfig{}, watermillLoggerAdapter)
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
			slogLogger.Debug("Received message", "metadata", msg.Metadata)
			return h(msg)
		}
	})

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			// We are using one topic for all events to maintain the order of events.
			return topicName, nil
		},
		Marshaler: cqrsMarshaler,
		Logger:    watermillLoggerAdapter,
	})
	if err != nil {
		panic(err)
	}

	kafkaConsumerConfig := sarama.NewConfig()
	kafkaConsumerConfig.Consumer.Return.Errors = true
	kafkaConsumerConfig.Version = sarama.V4_0_0_0
	kafkaConsumerConfig.ClientID = "go-cqrs-consumer"

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		cqrsRouter,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return topicName, nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						Brokers:               kafkaBootstrapServers,
						OverwriteSaramaConfig: kafkaConsumerConfig,
						ConsumerGroup:         params.EventGroupName,
						Unmarshaler:           kafkaMarshaler,
						NackResendSleep:       time.Millisecond * 100,
						ReconnectRetrySleep:   time.Second,
					},
					watermillLogger,
				)
			},
			Marshaler: cqrsMarshaler,
			Logger:    watermillLoggerAdapter,
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

	slogLogger.Info("Starting service")

	// https://gin-gonic.com/en/docs/examples/graceful-restart-or-stop/
	gin.SetMode(gin.ReleaseMode)
	ginRouter := gin.New()
	ginRouter.Use(sloggin.New(slogLogger))
	ginRouter.Use(gin.Recovery())

	makeHttpHandlers(ginRouter, slogLogger, eventBus, subscriberProjection, activityProjection)

	httpServer := &http.Server{
		Addr:           ":8080",
		Handler:        ginRouter.Handler(),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	appCtx, appCtxCancel := context.WithCancel(context.Background())

	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			slogLogger.Info("Http server is closed")
		} else if err != nil {
			slogLogger.Error("Got http server error", "err", err)
			appCtxCancel()
		}
	}()

	go func() {
		err := cqrsRouter.Run(context.Background())
		if err != nil {
			slogLogger.Error("Got cqrs error", "err", err)
			appCtxCancel()
		}
	}()

	select {
	case <-sigterm:
		slogLogger.Info("terminating: via signal")
		closeResources(slogLogger, httpServer, cqrsRouter, db)
		return
	case <-appCtx.Done():
		slogLogger.Info("terminating: due to error")
		closeResources(slogLogger, httpServer, cqrsRouter, db)
		return
	}
}

func makeHttpHandlers(ginRouter *gin.Engine, slogLogger *slog.Logger, eventBus *cqrs.EventBus, subscriberProjection *SubscriberProjection, activityProjection *ActivityTimelineProjection) {
	ginRouter.POST("/subscribe", func(g *gin.Context) {
		subscriberID := watermill.NewUUID()

		c := Subscribe{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
		}

		err := c.Handle(g.Request.Context(), eventBus, subscriberProjection)

		if err != nil {
			slogLogger.Error("Error sending Subscribe command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		m := map[string]string{
			"id": subscriberID,
		}

		g.JSON(http.StatusOK, m)
	})

	ginRouter.PUT("/update/:id", func(g *gin.Context) {
		subscriberID := g.Param("id")

		c := UpdateEmail{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
			NewEmail:     fmt.Sprintf("updated%d@example.com", time.Now().UTC().Unix()),
		}
		err := c.Handle(g.Request.Context(), eventBus, subscriberProjection)
		if err != nil {
			slogLogger.Error("Error sending UpdateEmail command", "err", err)
			m := map[string]string{
				"msg": err.Error(),
			}
			g.JSON(http.StatusBadRequest, m)
			return
		}
	})

	ginRouter.POST("/unsubscribe/:id", func(g *gin.Context) {
		subscriberID := g.Param("id")

		c := Unsubscribe{
			Metadata:     GenerateMessageMetadata(subscriberID),
			SubscriberId: subscriberID,
		}
		err := c.Handle(g.Request.Context(), eventBus, subscriberProjection)
		if err != nil {
			slogLogger.Error("Error sending Unsubscribe command", "err", err)
			m := map[string]string{
				"msg": err.Error(),
			}
			g.JSON(http.StatusBadRequest, m)
			return
		}
	})

	ginRouter.GET("/subscribers", func(g *gin.Context) {
		subscribers, err := subscriberProjection.GetSubscribers(g.Request.Context())
		if err != nil {
			slogLogger.Error("Error getting subscribers", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, subscribers)
	})

	ginRouter.GET("/activities", func(g *gin.Context) {
		activities, err := activityProjection.GetActivities(g.Request.Context())
		if err != nil {
			slogLogger.Error("Error getting activities", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, activities)
	})
}

func closeResources(slogLogger *slog.Logger, httpServer *http.Server, cqrsRouter *message.Router, db *sql.DB) {
	err := httpServer.Shutdown(context.Background())
	if err != nil {
		slogLogger.Error("error during close http server", "err", err)
	} else {
		slogLogger.Info("http server was closed successfully")
	}

	err = cqrsRouter.Close()
	if err != nil {
		slogLogger.Error("error during close cqrs", "err", err)
	} else {
		slogLogger.Info("cqrs was closed successfully")
	}

	err = db.Close()
	if err != nil {
		slogLogger.Error("error during close database", "err", err)
	} else {
		slogLogger.Info("database was closed successfully")
	}
}
