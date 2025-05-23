package handlers

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/fx"
	"log/slog"
	"net/http"
	"slices"
	"time"
)

type ChatCreateDto struct {
	Title          string  `json:"title"`
	ParticipantIds []int64 `json:"participantIds"`
}

type ChatEditDto struct {
	Id int64 `json:"id"`
	ChatCreateDto
}

type MessageCreateDto struct {
	Content string `json:"content"`
}

type ParticipantAddDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}

type ParticipantRemoveDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}

func makeHttpHandlers(ginRouter *gin.Engine, slogLogger *slog.Logger, eventBus cqrs.EventBusInterface, dbWrapper *db.DB, commonProjection *cqrs.CommonProjection) {
	ginRouter.POST("/chat", func(g *gin.Context) {

		ccd := new(ChatCreateDto)

		err := g.Bind(ccd)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding ChatCreateDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.ChatCreate{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			Title:          ccd.Title,
			ParticipantIds: ccd.ParticipantIds,
		}

		if !slices.Contains(cc.ParticipantIds, userId) {
			cc.ParticipantIds = append(cc.ParticipantIds, userId)
		}

		chatId, err := cc.Handle(g.Request.Context(), eventBus, dbWrapper, commonProjection)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatCreate command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		m := IdResponse{Id: chatId}

		g.JSON(http.StatusOK, m)
	})

	ginRouter.PUT("/chat", func(g *gin.Context) {
		ccd := new(ChatEditDto)

		err := g.Bind(ccd)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding ChatEditDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.ChatEdit{
			AdditionalData:      cqrs.GenerateMessageAdditionalData(),
			ChatId:              ccd.Id,
			Title:               ccd.Title,
			ParticipantIdsToAdd: ccd.ParticipantIds,
		}

		err = cc.Handle(g.Request.Context(), eventBus, commonProjection)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatEdit command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.DELETE("/chat/:id", func(g *gin.Context) {

		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.ChatRemove{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			ChatId:         chatId,
		}

		err = cc.Handle(g.Request.Context(), eventBus, commonProjection)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatRemove command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.PUT("/chat/:id/participant", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		ccd := new(ParticipantAddDto)

		err = g.Bind(ccd)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding ParticipantAddDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.ParticipantAdd{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			ParticipantIds: ccd.ParticipantIds,
			ChatId:         chatId,
		}

		err = cc.Handle(g.Request.Context(), eventBus)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ParticipantAdd command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.DELETE("/chat/:id/participant", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		ccd := new(ParticipantRemoveDto)

		err = g.Bind(ccd)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding ParticipantRemoveDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.ParticipantRemove{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			ParticipantIds: ccd.ParticipantIds,
			ChatId:         chatId,
		}

		err = cc.Handle(g.Request.Context(), eventBus)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ParticipantRemove command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.DELETE("/chat/:id/message/:messageId", func(g *gin.Context) {
		cid := g.Param("id")
		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mid := g.Param("messageId")
		messageId, err := utils.ParseInt64(mid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding messageId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.MessageRemove{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			MessageId:      messageId,
			ChatId:         chatId,
		}

		err = cc.Handle(g.Request.Context(), eventBus, commonProjection, userId)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending MessageRemove command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.PUT("/chat/:id/pin", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		p := g.Query("pin")

		pin := utils.GetBoolean(p)

		userId, err := getUserId(g)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.ChatPin{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			ChatId:         chatId,
			Pin:            pin,
			ParticipantId:  userId,
		}

		err = cc.Handle(g.Request.Context(), eventBus)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending ChatPin command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.POST("/chat/:id/message", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mcd := new(MessageCreateDto)

		err = g.Bind(mcd)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding MessageCreateDto", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		cc := cqrs.MessagePost{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			ChatId:         chatId,
			Content:        mcd.Content,
			OwnerId:        userId,
		}

		mid, err := cc.Handle(g.Request.Context(), eventBus, dbWrapper, commonProjection)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending MessagePost command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		m := IdResponse{Id: mid}

		g.JSON(http.StatusOK, m)
	})

	ginRouter.PUT("/chat/:id/message/:messageId/read", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mid := g.Param("messageId")

		messageId, err := utils.ParseInt64(mid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding messageId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		userId, err := getUserId(g)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		mr := cqrs.MessageRead{
			AdditionalData: cqrs.GenerateMessageAdditionalData(),
			ChatId:         chatId,
			MessageId:      messageId,
			ParticipantId:  userId,
		}

		err = mr.Handle(g.Request.Context(), eventBus, commonProjection)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error sending MessageRead command", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		g.Status(http.StatusOK)
	})

	ginRouter.GET("/chat/search", func(g *gin.Context) {
		userId, err := getUserId(g)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error parsing UserId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		chats, err := commonProjection.GetChats(g.Request.Context(), userId)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error getting chats", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, chats)
	})

	ginRouter.GET("/chat/:id/participants", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		participants, err := commonProjection.GetParticipants(g.Request.Context(), chatId)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error getting participants", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, participants)
	})

	ginRouter.GET("/chat/:id/message/search", func(g *gin.Context) {
		cid := g.Param("id")

		chatId, err := utils.ParseInt64(cid)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error binding chatId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}

		messages, err := commonProjection.GetMessages(g.Request.Context(), chatId)
		if err != nil {
			logger.LogWithTrace(g.Request.Context(), slogLogger).Error("Error getting messages", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		g.JSON(http.StatusOK, messages)
	})

	ginRouter.GET("/internal/health", func(g *gin.Context) {
		g.Status(http.StatusOK)
	})
}

type IdResponse struct {
	Id int64 `json:"id"`
}

func getUserId(g *gin.Context) (int64, error) {
	uh := g.Request.Header.Get("X-UserId")
	return utils.ParseInt64(uh)
}

func ConfigureHttpServer(
	cfg *config.AppConfig,
	slogLogger *slog.Logger,
	eventBus *cqrs.PartitionAwareEventBus,
	db *db.DB,
	commonProjection *cqrs.CommonProjection,
	lc fx.Lifecycle,
) *http.Server {
	// https://gin-gonic.com/en/docs/examples/graceful-restart-or-stop/
	gin.SetMode(gin.ReleaseMode)
	ginRouter := gin.New()
	ginRouter.Use(otelgin.Middleware(app.TRACE_RESOURCE))
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
func StructuredLogMiddleware(slogLogger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceId := logger.GetTraceId(c.Request.Context())

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
			logger.LogFieldTraceId, traceId,
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
		traceId := logger.GetTraceId(c.Request.Context())

		c.Writer.Header().Set("trace-id", traceId)

		// Process Request
		c.Next()

	}
}

func RunHttpServer(
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
