package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"io"
	"io/ioutil"
	"log/slog"
	"main.go/config"
	"main.go/cqrs"
	"main.go/handlers"
	"main.go/logger"
	"main.go/utils"
	"net/http"
)

type RestClient struct {
	*http.Client
	tracer trace.Tracer
	lgr    *slog.Logger
	cfg    *config.AppConfig
}

func NewRestClient(cfg *config.AppConfig, lgr *slog.Logger) *RestClient {
	tr := &http.Transport{
		MaxIdleConns:       cfg.RestClientConfig.MaxIdleConns,
		IdleConnTimeout:    cfg.RestClientConfig.IdleConnTimeout,
		DisableCompression: cfg.RestClientConfig.DisableCompression,
	}
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	trR := otelhttp.NewTransport(tr)
	client := &http.Client{Transport: trR}
	trcr := otel.Tracer("rest/client")

	return &RestClient{client, trcr, lgr, cfg}
}

func (rc *RestClient) CreateChat(ctx context.Context, behalfUserId int64, chatName string) (int64, error) {
	req := handlers.ChatCreateDto{
		Title: chatName,
	}
	resp, err := query[handlers.ChatCreateDto, handlers.IdResponse](ctx, rc, behalfUserId, "POST", "/chat", "chat.Create", &req)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (rc *RestClient) GetChatsByUserId(ctx context.Context, behalfUserId int64) ([]cqrs.ChatViewDto, error) {
	return query[any, []cqrs.ChatViewDto](ctx, rc, behalfUserId, "GET", "/chat/search", "chat.Search", nil)
}

func (rc *RestClient) CreateMessage(ctx context.Context, behalfUserId int64, chatId int64, text string) (int64, error) {
	req := handlers.MessageCreateDto{
		Content: text,
	}
	resp, err := query[handlers.MessageCreateDto, handlers.IdResponse](ctx, rc, behalfUserId, "POST", "/chat/"+utils.ToString(chatId)+"/message", "message.Create", &req)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (rc *RestClient) DeleteMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	httpResp, err := queryRaw[any](ctx, rc, behalfUserId, "DELETE", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId), "message.Delete", nil)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	return nil
}

func (rc *RestClient) GetMessages(ctx context.Context, behalfUserId int64, chatId int64) ([]cqrs.MessageViewDto, error) {
	return query[any, []cqrs.MessageViewDto](ctx, rc, behalfUserId, "GET", "/chat/"+utils.ToString(chatId)+"/message/search", "message.Search", nil)
}

func (rc *RestClient) AddChatParticipants(ctx context.Context, chatId int64, participantIds []int64) error {
	req := handlers.ParticipantAddDto{
		ParticipantIds: participantIds,
	}
	httpResp, err := queryRaw[handlers.ParticipantAddDto](ctx, rc, 0, "PUT", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Add", &req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	return nil
}

func (rc *RestClient) GetChatParticipants(ctx context.Context, chatId int64) ([]int64, error) {
	return query[any, []int64](ctx, rc, 0, "GET", "/chat/"+utils.ToString(chatId)+"/participants", "participants.Get", nil)
}

func (rc *RestClient) ReadMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	httpResp, err := queryRaw[any](ctx, rc, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId)+"/read", "message.Read", nil)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	return nil
}

func (rc *RestClient) HealthCheck(ctx context.Context) error {
	httpResp, err := queryRaw[any](ctx, rc, 0, "GET", "/internal/health", "internal.HealthCheck", nil)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	return nil
}

// You should call 	defer httpResp.Body.Close()
func queryRaw[ReqDto any](ctx context.Context, rc *RestClient, behalfUserId int64, method, url, opName string, req *ReqDto) (*http.Response, error) {
	contentType := "application/json;charset=UTF-8"
	fullUrl := utils.StringToUrl("http://localhost" + rc.cfg.HttpServerConfig.Address + url)

	requestHeaders := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept":          {contentType},
		"Content-Type":    {contentType},
		"X-UserId":        {utils.ToString(behalfUserId)},
	}

	httpReq := &http.Request{
		Method: method,
		Header: requestHeaders,
		URL:    fullUrl,
	}

	if req != nil {
		bytesData, err := json.Marshal(req)
		if err != nil {
			logger.LogWithTrace(ctx, rc.lgr).Error(fmt.Sprintf("Failed during marshalling request body for %v:", opName), "err", err)
			return nil, err
		}
		reader := bytes.NewReader(bytesData)

		httpReq.Body = ioutil.NopCloser(reader)
	}

	ctx, span := rc.tracer.Start(ctx, opName)
	defer span.End()
	httpReq = httpReq.WithContext(ctx)
	httpResp, err := rc.Do(httpReq)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn(fmt.Sprintf("Failed to request %v response:", opName), "err", err)
		return nil, err
	}
	code := httpResp.StatusCode
	if !(code >= 200 && code < 300) {
		logger.LogWithTrace(ctx, rc.lgr).Warn(fmt.Sprintf("%v response responded non-2xx code: ", opName), "code", code)
		return nil, errors.New(fmt.Sprintf("%v response responded non-2xx code", opName))
	}

	return httpResp, err
}

func query[ReqDto any, ResDto any](ctx context.Context, rc *RestClient, behalfUserId int64, method, url, opName string, req *ReqDto) (ResDto, error) {
	var resp ResDto
	var err error
	httpResp, err := queryRaw(ctx, rc, behalfUserId, method, url, opName, req)
	if err != nil {
		return resp, err
	}
	defer httpResp.Body.Close()

	bodyBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn(fmt.Sprintf("Failed to decode %v response:", opName), "err", err)
		return resp, err
	}

	if err = json.Unmarshal(bodyBytes, &resp); err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error(fmt.Sprintf("Failed to parse %v response:", opName), "err", err)
		return resp, err
	}
	return resp, nil
}
