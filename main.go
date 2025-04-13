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
	wotel "github.com/voi-oss/watermill-opentelemetry/pkg/opentelemetry"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	jaegerPropagator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const TRACE_RESOURCE = "go-cqrs-example"
const LogFieldTraceId = "trace_id"

// TODO think about sequence restoration - introduce an new event
// TODO handle duplicated events - change inserts to upserts - when we pasuse for long time in the consumer
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

	traceExporterConn, err := grpc.DialContext(context.Background(), "localhost:4317", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		panic(err)
	}

	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithGRPCConn(traceExporterConn))
	if err != nil {
		panic(err)
	}
	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(TRACE_RESOURCE),
	)
	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(batchSpanProcessor),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	aJaegerPropagator := jaegerPropagator.Jaeger{}
	// register jaeger propagator
	otel.SetTextMapPropagator(aJaegerPropagator)

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

	decoratedPublisher := wotel.NewPublisherDecorator(publisher)

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
	cqrsRouter.AddMiddleware(wotel.Trace())
	cqrsRouter.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			slogLogger.Debug("Received message", "metadata", msg.Metadata)
			return h(msg)
		}
	})

	eventBus, err := cqrs.NewEventBusWithConfig(decoratedPublisher, cqrs.EventBusConfig{
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
	ginRouter.Use(otelgin.Middleware(TRACE_RESOURCE))
	ginRouter.Use(StructuredLogMiddleware(slogLogger))
	ginRouter.Use(WriteTraceToHeaderMiddleware())
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
		closeResources(slogLogger, httpServer, cqrsRouter, db, tp, traceExporterConn)
		return
	case <-appCtx.Done():
		slogLogger.Info("terminating: due to error")
		closeResources(slogLogger, httpServer, cqrsRouter, db, tp, traceExporterConn)
		os.Exit(1)
		return
	}
}

func makeHttpHandlers(ginRouter *gin.Engine, slogLogger *slog.Logger, eventBus *cqrs.EventBus, subscriberProjection *SubscriberProjection, activityProjection *ActivityTimelineProjection) {
	ginRouter.POST("/subscribe", func(g *gin.Context) {
		subscriberID := watermill.NewUUID()

		c := Subscribe{
			Metadata:     GenerateMessageMetadata(g.Request.Context(), subscriberID),
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
			Metadata:     GenerateMessageMetadata(g.Request.Context(), subscriberID),
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
			Metadata:     GenerateMessageMetadata(g.Request.Context(), subscriberID),
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

func closeResources(slogLogger *slog.Logger, httpServer *http.Server, cqrsRouter *message.Router, db *sql.DB, tp *sdktrace.TracerProvider, traceExporterConn *grpc.ClientConn) {
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

	err = tp.Shutdown(context.Background())
	if err != nil {
		slogLogger.Error("error during close tracer", "err", err)
	} else {
		slogLogger.Info("tracer was closed successfully")
	}

	err = traceExporterConn.Close()
	if err != nil {
		slogLogger.Error("error during close trace exporter connection", "err", err)
	} else {
		slogLogger.Info("trace exporter connection was closed successfully")
	}
}

var randSource = rand.New(rand.NewSource(1))

func GetTraceId(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	return sc.TraceID().String()
}

func CreateSpan(ctx context.Context, traceId string, slogLogger *slog.Logger) context.Context {
	traceID, err := trace.TraceIDFromHex(traceId)
	if err != nil {
		slogLogger.Error("Unable to extract traceId from", "hex", traceID)
		return ctx
	}

	spanID := trace.SpanID{}
	_, _ = randSource.Read(spanID[:])

	// https://stackoverflow.com/questions/77161111/golang-set-custom-traceid-and-spanid-in-opentelemetry/77176591#77176591
	// ContextWithRemoteSpanContext
	ctxRet := trace.ContextWithSpanContext(
		ctx,
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		}),
	)
	return ctxRet
}

func StructuredLogMiddleware(slogLogger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceId := GetTraceId(c.Request.Context())

		// Start timer
		start := time.Now()

		// Process Request
		c.Next()

		// Stop timer
		end := time.Now()

		duration := end.Sub(start)

		entries := []any{
			"client_ip", c.ClientIP(),
			"duration", duration,
			"method", c.Request.Method,
			"path", c.Request.RequestURI,
			"status", c.Writer.Status(),
			"referrer", c.Request.Referer(),
			LogFieldTraceId, traceId,
		}

		if c.Writer.Status() >= 500 {
			slogLogger.Error("Request", entries...)
		} else {
			slogLogger.Info("Request", entries...)
		}
	}
}

func WriteTraceToHeaderMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceId := GetTraceId(c.Request.Context())

		c.Writer.Header().Set("trace-id", traceId)

		// Process Request
		c.Next()

	}
}
