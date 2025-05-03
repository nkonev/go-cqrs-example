package main

import (
	"bytes"
	"context"
	"embed"
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
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	jaegerPropagator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const TRACE_RESOURCE = "chat-cqrs-example"
const LogFieldTraceId = "trace_id"

// TODO change chat
// TODO delete chat
// TODO remove participant from chat
// TODO consider batching for adding participants
// TODO draw the last message in user's chat view
type KafkaConfig struct {
	BootstrapServers    []string            `mapstructure:"bootstrapServers"`
	Topic               string              `mapstructure:"topic"`
	NumPartitions       int32               `mapstructure:"numPartitions"`
	ReplicationFactor   int16               `mapstructure:"replicationFactor"`
	Retention           string              `mapstructure:"retention"`
	ConsumerGroup       string              `mapstructure:"consumerGroup"`
	KafkaProducerConfig KafkaProducerConfig `mapstructure:"producer"`
	KafkaConsumerConfig KafkaConsumerConfig `mapstructure:"consumer"`
}

type KafkaProducerConfig struct {
	RetryMax      int           `mapstructure:"retryMax"`
	ReturnSuccess bool          `mapstructure:"returnSuccess"`
	RetryBackoff  time.Duration `mapstructure:"retryBackoff"`
	ClientId      string        `mapstructure:"clientId"`
}

type KafkaConsumerConfig struct {
	ReturnErrors        bool          `mapstructure:"returnErrors"`
	ClientId            string        `mapstructure:"clientId"`
	NackResendSleep     time.Duration `mapstructure:"nackResendSleep"`
	ReconnectRetrySleep time.Duration `mapstructure:"reconnectRetrySleep"`
}

type OtlpConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

type HttpServerConfig struct {
	Address        string        `mapstructure:"address"`
	ReadTimeout    time.Duration `mapstructure:"readTimeout"`
	WriteTimeout   time.Duration `mapstructure:"writeTimeout"`
	MaxHeaderBytes int           `mapstructure:"maxHeaderBytes"`
}

type PostgreSQLConfig struct {
	Url                string        `mapstructure:"url"`
	MaxOpenConnections int           `mapstructure:"maxOpenConnections"`
	MaxIdleConnections int           `mapstructure:"maxIdleConnections"`
	MaxLifetime        time.Duration `mapstructure:"maxLifetime"`
}

type AppConfig struct {
	KafkaConfig      KafkaConfig      `mapstructure:"kafka"`
	OtlpConfig       OtlpConfig       `mapstructure:"otlp"`
	PostgreSQLConfig PostgreSQLConfig `mapstructure:"postgresql"`
	HttpServerConfig HttpServerConfig `mapstructure:"server"`
}

//go:embed config
var configFs embed.FS

func createTypedConfig() (*AppConfig, error) {
	conf := AppConfig{}
	viper.SetConfigType("yaml")

	if embedBytes, err := configFs.ReadFile("config/config.yml"); err != nil {
		panic(fmt.Errorf("Fatal error during reading embedded config file: %s \n", err))
	} else if err := viper.ReadConfig(bytes.NewBuffer(embedBytes)); err != nil {
		panic(fmt.Errorf("Fatal error during viper reading embedded config file: %s \n", err))
	}

	viper.SetEnvPrefix(strings.ToUpper(TRACE_RESOURCE))
	viper.AutomaticEnv()
	err := viper.GetViper().Unmarshal(&conf)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("config file loaded failed. %v\n", err))
	}

	return &conf, nil
}

func configureDatabase(
	slogLogger *slog.Logger,
	cfg *AppConfig,
	tp *sdktrace.TracerProvider, // to configure it after tracing infrastructure
	lc fx.Lifecycle,
) (*DB, error) {

	db, err := otelsql.Open("pgx", cfg.PostgreSQLConfig.Url, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	if err != nil {
		return nil, err
	}
	err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(cfg.PostgreSQLConfig.MaxLifetime)
	db.SetMaxIdleConns(cfg.PostgreSQLConfig.MaxIdleConnections)
	db.SetMaxOpenConns(cfg.PostgreSQLConfig.MaxOpenConnections)
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dbWrapper := &DB{
		DB:  db,
		lgr: slogLogger,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping database")
			if err := db.Close(); err != nil {
				slogLogger.Error("Error shutting down database", "err", err)
			}
			return nil
		},
	})

	return dbWrapper, nil
}

func configureTracePropagator() propagation.TextMapPropagator {
	return jaegerPropagator.Jaeger{}
}

func configureTraceProvider(
	slogLogger *slog.Logger,
	propagator propagation.TextMapPropagator,
	exporter *otlptrace.Exporter,
	lc fx.Lifecycle,
) *sdktrace.TracerProvider {
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

	// register jaeger propagator
	otel.SetTextMapPropagator(propagator)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping trace provider")
			if err := tp.Shutdown(context.Background()); err != nil {
				slogLogger.Error("Error shutting trace provider", "err", err)
			}
			return nil
		},
	})
	return tp
}

func configureTraceExporter(
	slogLogger *slog.Logger,
	cfg *AppConfig,
	lc fx.Lifecycle,
) (*otlptrace.Exporter, error) {
	traceExporterConn, err := grpc.DialContext(context.Background(), cfg.OtlpConfig.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithGRPCConn(traceExporterConn))
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping trace exporter")

			if err := exporter.Shutdown(ctx); err != nil {
				slogLogger.Error("Error shutting down trace exporter", "err", err)
			}

			if err := traceExporterConn.Close(); err != nil {
				slogLogger.Error("Error shutting down trace exporter connection", "err", err)
			}
			return nil
		},
	})

	return exporter, err
}

func configureKafkaAdmin(
	slogLogger *slog.Logger,
	cfg *AppConfig,
	lc fx.Lifecycle,
) (sarama.ClusterAdmin, error) {
	kafkaAdminConfig := sarama.NewConfig()
	kafkaAdminConfig.Version = sarama.V4_0_0_0

	kafkaAdmin, err := sarama.NewClusterAdmin(cfg.KafkaConfig.BootstrapServers, kafkaAdminConfig)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping kafka admin")

			if err := kafkaAdmin.Close(); err != nil {
				slogLogger.Error("Error shutting down kafka admin", "err", err)
			}
			return nil
		},
	})

	return kafkaAdmin, nil
}

func runCreateTopic(
	slogLogger *slog.Logger,
	cfg *AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	retention := cfg.KafkaConfig.Retention
	topicName := cfg.KafkaConfig.Topic
	consumerGroupName := cfg.KafkaConfig.ConsumerGroup
	slogLogger.Info("Creating topic", "topic", topicName)

	err := kafkaAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     cfg.KafkaConfig.NumPartitions,
		ReplicationFactor: cfg.KafkaConfig.ReplicationFactor,
		ConfigEntries: map[string]*string{
			// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
			"retention.ms": &retention,
		},
	}, false)
	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
		slogLogger.Info("Topic is already exists", "topic", topicName)
	} else if err != nil {
		return err
	} else {
		slogLogger.Info("Topic was successfully created", "topic", topicName)
	}

	offsets, err := kafkaAdmin.ListConsumerGroupOffsets(consumerGroupName, map[string][]int32{topicName: []int32{0, 1, 2}})
	if err != nil {
		return err
	}
	offss := offsets.Blocks[topicName]
	for p, o := range offss {
		slogLogger.Info("Got", "partition", p, "offset", o.Offset)
	}
	return nil
}

func configureKafkaMarshaller(
	slogLogger *slog.Logger,
) kafka.MarshalerUnmarshaler {
	// This marshaler converts Watermill messages to Kafka messages.
	// We are using it to add partition key to the Kafka message.
	return kafka.NewWithPartitioningMarshaler(GenerateKafkaPartitionKey(slogLogger))
}

func configureWatermillLogger(
	slogLogger *slog.Logger,
) watermill.LoggerAdapter {
	return watermill.NewSlogLoggerWithLevelMapping(
		slogLogger.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)
}

func configurePublisher(
	cfg *AppConfig,
	watermillLogger watermill.LoggerAdapter,
	propagator propagation.TextMapPropagator,
	tp *sdktrace.TracerProvider, // to configure it after tracing infrastructure
	kafkaMarshaler kafka.MarshalerUnmarshaler,
) (message.Publisher, error) {
	// You can use any Pub/Sub implementation from here: https://watermill.io/pubsubs/
	kafkaProducerConfig := sarama.NewConfig()
	kafkaProducerConfig.Producer.Retry.Max = cfg.KafkaConfig.KafkaProducerConfig.RetryMax
	kafkaProducerConfig.Producer.Return.Successes = cfg.KafkaConfig.KafkaProducerConfig.ReturnSuccess
	kafkaProducerConfig.Version = sarama.V4_0_0_0
	kafkaProducerConfig.Metadata.Retry.Backoff = cfg.KafkaConfig.KafkaProducerConfig.RetryBackoff
	kafkaProducerConfig.ClientID = cfg.KafkaConfig.KafkaProducerConfig.ClientId

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               cfg.KafkaConfig.BootstrapServers,
			OverwriteSaramaConfig: kafkaProducerConfig,
			Marshaler:             kafkaMarshaler,
		},
		watermillLogger,
	)
	if err != nil {
		return nil, err
	}

	publisherDecorator := wotel.NewPublisherDecorator(publisher, wotel.WithTextMapPropagator(propagator))

	return publisherDecorator, nil
}

func configureCqrsRouter(
	slogLogger *slog.Logger,
	watermillLoggerAdapter watermill.LoggerAdapter,
	propagator propagation.TextMapPropagator,
	tp *sdktrace.TracerProvider, // to configure it after tracing infrastructure
	lc fx.Lifecycle,
) (*message.Router, error) {
	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	cqrsRouter, err := message.NewRouter(message.RouterConfig{}, watermillLoggerAdapter)
	if err != nil {
		return nil, err
	}

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	cqrsRouter.AddMiddleware(middleware.Recoverer)
	cqrsRouter.AddMiddleware(wotel.Trace(wotel.WithTextMapPropagator(propagator)))
	cqrsRouter.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			LogWithTrace(msg.Context(), slogLogger).Debug("Received message", "metadata", msg.Metadata)
			return h(msg)
		}
	})

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping cqrs router")

			if err := cqrsRouter.Close(); err != nil {
				slogLogger.Error("Error shutting down router", "err", err)
			}
			return nil
		},
	})

	return cqrsRouter, nil
}

func configureCqrsMarshaller() *CqrsMarshalerDecorator {
	// We are decorating ProtobufMarshaler to add extra metadata to the message.
	return &CqrsMarshalerDecorator{
		cqrs.JSONMarshaler{
			// It will generate topic names based on the event/command type.
			// So for example, for "RoomBooked" name will be "RoomBooked".
			GenerateName: cqrs.NamedStruct(func(v interface{}) string {
				panic(fmt.Sprintf("not implemented Name() for %T", v))
			}),
		}}
}

func configureEventBus(
	cfg *AppConfig,
	publisher message.Publisher,
	cqrsMarshaler *CqrsMarshalerDecorator,
	watermillLoggerAdapter watermill.LoggerAdapter,
) (*PartitionAwareEventBus, error) {
	eventBusRoot, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			// We are using one topic for all events to maintain the order of events.
			return cfg.KafkaConfig.Topic, nil
		},
		Marshaler: cqrsMarshaler,
		Logger:    watermillLoggerAdapter,
	})
	if err != nil {
		return nil, err
	}

	return &PartitionAwareEventBus{eventBusRoot}, nil
}

func configureEventProcessor(
	cfg *AppConfig,
	cqrsRouter *message.Router,
	watermillLoggerAdapter watermill.LoggerAdapter,
	kafkaMarshaler kafka.MarshalerUnmarshaler,
	cqrsMarshaler *CqrsMarshalerDecorator,
	commonProjection *CommonProjection,
) (*cqrs.EventGroupProcessor, error) {
	kafkaConsumerConfig := sarama.NewConfig()
	kafkaConsumerConfig.Consumer.Return.Errors = cfg.KafkaConfig.KafkaConsumerConfig.ReturnErrors
	kafkaConsumerConfig.Version = sarama.V4_0_0_0
	kafkaConsumerConfig.ClientID = cfg.KafkaConfig.KafkaConsumerConfig.ClientId

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		cqrsRouter,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				return cfg.KafkaConfig.Topic, nil
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						Brokers:               cfg.KafkaConfig.BootstrapServers,
						OverwriteSaramaConfig: kafkaConsumerConfig,
						ConsumerGroup:         params.EventGroupName,
						Unmarshaler:           kafkaMarshaler,
						NackResendSleep:       cfg.KafkaConfig.KafkaConsumerConfig.NackResendSleep,
						ReconnectRetrySleep:   cfg.KafkaConfig.KafkaConsumerConfig.ReconnectRetrySleep,
					},
					watermillLoggerAdapter,
				)
			},
			Marshaler: cqrsMarshaler,
			Logger:    watermillLoggerAdapter,
		},
	)
	if err != nil {
		return nil, err
	}

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		cfg.KafkaConfig.ConsumerGroup,
		cqrs.NewGroupEventHandler(commonProjection.OnChatCreated),
		cqrs.NewGroupEventHandler(commonProjection.OnParticipantAdded),
		cqrs.NewGroupEventHandler(commonProjection.OnChatPinned),
		cqrs.NewGroupEventHandler(commonProjection.OnMessageCreated),
		cqrs.NewGroupEventHandler(commonProjection.OnUnreadMessageIncreased),
		cqrs.NewGroupEventHandler(commonProjection.OnUnreadMessageReaded),
	)
	if err != nil {
		return nil, err
	}

	return eventProcessor, nil
}

func configureCommonProjection(
	db *DB,
	slogLogger *slog.Logger,
) *CommonProjection {
	return NewCommonProjection(db, slogLogger)
}

func configureHttpServer(
	cfg *AppConfig,
	slogLogger *slog.Logger,
	eventBus *PartitionAwareEventBus,
	db *DB,
	commonProjection *CommonProjection,
	lc fx.Lifecycle,
) *http.Server {
	// https://gin-gonic.com/en/docs/examples/graceful-restart-or-stop/
	gin.SetMode(gin.ReleaseMode)
	ginRouter := gin.New()
	ginRouter.Use(otelgin.Middleware(TRACE_RESOURCE))
	ginRouter.Use(StructuredLogMiddleware(slogLogger))
	ginRouter.Use(WriteTraceToHeaderMiddleware())
	ginRouter.Use(gin.Recovery())

	makeHttpHandlers(ginRouter, slogLogger, eventBus, db, commonProjection)

	httpServer := &http.Server{
		Addr:           cfg.HttpServerConfig.Address,
		Handler:        ginRouter.Handler(),
		ReadTimeout:    cfg.HttpServerConfig.ReadTimeout,
		WriteTimeout:   cfg.HttpServerConfig.WriteTimeout,
		MaxHeaderBytes: cfg.HttpServerConfig.MaxHeaderBytes,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping http server")

			if err := httpServer.Shutdown(context.Background()); err != nil {
				slogLogger.Error("Error shutting http server", "err", err)
			}
			return nil
		},
	})

	return httpServer
}

func runHttpServer(
	slogLogger *slog.Logger,
	httpServer *http.Server,
) {
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			slogLogger.Info("Http server is closed")
		} else if err != nil {
			slogLogger.Error("Got http server error", "err", err)
		}
	}()
}

func runCqrsRouter(
	slogLogger *slog.Logger,
	cqrsRouter *message.Router,
	processor *cqrs.EventGroupProcessor, // to configure it before this
) error {

	go func() {
		err := cqrsRouter.Run(context.Background())
		if err != nil {
			slogLogger.Error("Got cqrs error", "err", err)
		}
	}()
	return nil
}

func main() {
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	appFx := fx.New(
		fx.Supply(slogLogger),
		fx.Provide(
			createTypedConfig,
			configureTracePropagator,
			configureTraceProvider,
			configureTraceExporter,
			configureDatabase,
			configureKafkaAdmin,
			configureKafkaMarshaller,
			configureWatermillLogger,
			configurePublisher,
			configureCqrsRouter,
			configureCqrsMarshaller,
			configureEventBus,
			configureEventProcessor,
			configureCommonProjection,
			configureHttpServer,
		),
		fx.Invoke(
			runCreateTopic,
			runHttpServer,
			runCqrsRouter,
		),
	)
	appFx.Run()
	slogLogger.Info("Exit program")
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
