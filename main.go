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
	"github.com/XSAM/otelsql"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4/stdlib"
	wotel "github.com/nkonev/watermill-opentelemetry/pkg/opentelemetry"
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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const TRACE_RESOURCE = "chat-cqrs-example"
const LogFieldTraceId = "trace_id"

// TODO unhardcode - use viper
// TODO mark message as read for user who composed it
// TODO change chat
// TODO delete chat
func main() {
	kafkaBootstrapServers := []string{"127.0.0.1:9092"}
	topicName := "events"
	consumerGroupName := "CommonProjection"

	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

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

	db, err := otelsql.Open("pgx", "postgres://postgres:postgresqlPassword@localhost:5432/postgres?sslmode=disable&application_name=cqrs-app", otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	if err != nil {
		panic(err)
	}
	err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
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

	dbWrapper := &DB{
		DB:  db,
		lgr: slogLogger,
	}

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
		NumPartitions:     3,
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

	offsets, err := kafkaAdmin.ListConsumerGroupOffsets(consumerGroupName, map[string][]int32{topicName: []int32{0, 1, 2}})
	if err != nil {
		panic(err)
	}
	offss := offsets.Blocks[topicName]
	for p, o := range offss {
		slogLogger.Info("Got", "partition", p, "offset", o.Offset)
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
		}}

	watermillLogger := watermill.NewSlogLoggerWithLevelMapping(
		slogLogger.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)

	// This marshaler converts Watermill messages to Kafka messages.
	// We are using it to add partition key to the Kafka message.
	kafkaMarshaler := kafka.NewWithPartitioningMarshaler(GenerateKafkaPartitionKey(slogLogger))

	// You can use any Pub/Sub implementation from here: https://watermill.io/pubsubs/
	kafkaProducerConfig := sarama.NewConfig()
	kafkaProducerConfig.Producer.Retry.Max = 10
	kafkaProducerConfig.Producer.Return.Successes = true
	kafkaProducerConfig.Version = sarama.V4_0_0_0
	kafkaProducerConfig.Metadata.Retry.Backoff = time.Second * 2
	kafkaProducerConfig.ClientID = "chat-producer"

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
			LogWithTrace(msg.Context(), slogLogger).Debug("Received message", "metadata", msg.Metadata)
			return h(msg)
		}
	})

	eventBusRoot, err := cqrs.NewEventBusWithConfig(decoratedPublisher, cqrs.EventBusConfig{
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

	eventBus := &PartitionAwareEventBus{eventBusRoot}

	kafkaConsumerConfig := sarama.NewConfig()
	kafkaConsumerConfig.Consumer.Return.Errors = true
	kafkaConsumerConfig.Version = sarama.V4_0_0_0
	kafkaConsumerConfig.ClientID = "chat-consumer"

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

	commonProjection := NewCommonProjection(dbWrapper, slogLogger)

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		consumerGroupName,
		cqrs.NewGroupEventHandler(commonProjection.OnChatCreated),
		cqrs.NewGroupEventHandler(commonProjection.OnParticipantAdded),
		cqrs.NewGroupEventHandler(commonProjection.OnChatPinned),
		cqrs.NewGroupEventHandler(commonProjection.OnMessageCreated),
		cqrs.NewGroupEventHandler(commonProjection.OnUnreadMessageIncreased),
		cqrs.NewGroupEventHandler(commonProjection.OnUnreadMessageReaded),
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

	makeHttpHandlers(ginRouter, slogLogger, eventBus, dbWrapper, commonProjection)

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

type ChatCreateDto struct {
	Title          string  `json:"title"`
	ParticipantIds []int64 `json:"participantIds"`
}

type MessageCreateDto struct {
	Content string `json:"content"`
}

type ParticipantAddDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}

func makeHttpHandlers(ginRouter *gin.Engine, slogLogger *slog.Logger, eventBus EventBusInterface, dbWrapper *DB, commonProjection *CommonProjection) {
	ginRouter.POST("/chat", func(g *gin.Context) {

		ccd := new(ChatCreateDto)

		err := g.Bind(ccd)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding ChatCreateDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := ChatCreate{
			AdditionalData: GenerateMessageAdditionalData(),
			Title:          ccd.Title,
			ParticipantIds: ccd.ParticipantIds,
		}

		if !Contains(cc.ParticipantIds, userId) {
			cc.ParticipantIds = append(cc.ParticipantIds, userId)
		}

		chatId, err := cc.Handle(g.Request.Context(), eventBus, dbWrapper, commonProjection)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatCreate command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		m := map[string]any{
			"id": chatId,
		}

		g.JSON(http.StatusOK, m)
	})

	ginRouter.PUT("/chat/:id/participant", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := ParseInt64(cid)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		ccd := new(ParticipantAddDto)

		err = g.Bind(ccd)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding ParticipantAddDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := ParticipantAdd{
			AdditionalData: GenerateMessageAdditionalData(),
			ParticipantIds: ccd.ParticipantIds,
			ChatId:         chatId,
		}

		err = cc.Handle(g.Request.Context(), eventBus)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatCreate command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.PUT("/chat/:id/pin", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := ParseInt64(cid)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		p := g.Query("pin")

		pin := GetBoolean(p)

		userId, err := getUserId(g)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := ChatPin{
			AdditionalData: GenerateMessageAdditionalData(),
			ChatId:         chatId,
			Pin:            pin,
			ParticipantId:  userId,
		}

		err = cc.Handle(g.Request.Context(), eventBus)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatPin command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.POST("/chat/:id/message", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := ParseInt64(cid)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mcd := new(MessageCreateDto)

		err = g.Bind(mcd)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding MessageCreateDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := MessagePost{
			AdditionalData: GenerateMessageAdditionalData(),
			ChatId:         chatId,
			Content:        mcd.Content,
			OwnerId:        userId,
		}

		mid, err := cc.Handle(g.Request.Context(), eventBus, dbWrapper, commonProjection)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending MessagePost command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		m := map[string]any{
			"id": mid,
		}

		g.JSON(http.StatusOK, m)
	})

	ginRouter.PUT("/chat/:id/message/:messageId/read", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := ParseInt64(cid)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mid := g.Param("messageId")

		messageId, err := ParseInt64(mid)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding messageId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mr := MessageRead{
			AdditionalData: GenerateMessageAdditionalData(),
			ChatId:         chatId,
			MessageId:      messageId,
			ParticipantId:  userId,
		}

		err = mr.Handle(g.Request.Context(), eventBus)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending MessageRead command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.GET("/chat/search", func(g *gin.Context) {
		userId, err := getUserId(g)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		chats, err := commonProjection.GetChats(g.Request.Context(), userId)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error getting chats", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, chats)
	})

	ginRouter.GET("/chat/:id/message/search", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := ParseInt64(cid)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		messages, err := commonProjection.GetMessages(g.Request.Context(), chatId)
		if err != nil {
			LogWithTrace(g.Request.Context(), slogLogger).Error("Error getting messages", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, messages)
	})
}

func getUserId(g *gin.Context) (int64, error) {
	uh := g.Request.Header.Get("X-UserId")
	return ParseInt64(uh)
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

func GetTraceId(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	return sc.TraceID().String()
}

func LogWithTrace(ctx context.Context, slogLogger *slog.Logger) *slog.Logger {
	return slogLogger.With(LogFieldTraceId, GetTraceId(ctx))
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
