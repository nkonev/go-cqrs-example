package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/handlers"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
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

func (rc *RestClient) ChangeChat(ctx context.Context, chatId int64, chatName string) error {
	req := handlers.ChatEditDto{
		Id: chatId,
		ChatCreateDto: handlers.ChatCreateDto{
			Title: chatName,
		},
	}
	err := queryNoResponse[handlers.ChatEditDto](ctx, rc, 0, "PUT", "/chat", "chat.Change", &req)
	if err != nil {
		return err
	}
	return nil
}

func (rc *RestClient) PinChat(ctx context.Context, behalfUserId int64, chatId int64, pin bool) error {
	return queryNoResponse[any](ctx, rc, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/pin?pin="+utils.ToString(pin), "chat.Pin", nil)
}

func (rc *RestClient) RemoveChat(ctx context.Context, chatId int64) error {
	return queryNoResponse[any](ctx, rc, 0, "DELETE", "/chat/"+utils.ToString(chatId), "chat.Delete", nil)
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
	return queryNoResponse[any](ctx, rc, behalfUserId, "DELETE", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId), "message.Delete", nil)
}

func (rc *RestClient) GetMessages(ctx context.Context, behalfUserId int64, chatId int64) ([]cqrs.MessageViewDto, error) {
	return query[any, []cqrs.MessageViewDto](ctx, rc, behalfUserId, "GET", "/chat/"+utils.ToString(chatId)+"/message/search", "message.Search", nil)
}

func (rc *RestClient) AddChatParticipants(ctx context.Context, chatId int64, participantIds []int64) error {
	req := handlers.ParticipantAddDto{
		ParticipantIds: participantIds,
	}
	return queryNoResponse[handlers.ParticipantAddDto](ctx, rc, 0, "PUT", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Add", &req)
}

func (rc *RestClient) RemoveChatParticipants(ctx context.Context, chatId int64, participantIds []int64) error {
	req := handlers.ParticipantRemoveDto{
		ParticipantIds: participantIds,
	}
	return queryNoResponse[handlers.ParticipantRemoveDto](ctx, rc, 0, "DELETE", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Delete", &req)
}

func (rc *RestClient) GetChatParticipants(ctx context.Context, chatId int64) ([]int64, error) {
	return query[any, []int64](ctx, rc, 0, "GET", "/chat/"+utils.ToString(chatId)+"/participants", "participants.Get", nil)
}

func (rc *RestClient) ReadMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, rc, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId)+"/read", "message.Read", nil)
}

func (rc *RestClient) HealthCheck(ctx context.Context) error {
	return queryNoResponse[any](ctx, rc, 0, "GET", "/internal/health", "internal.HealthCheck", nil)
}

// You should call 	defer httpResp.Body.Close()
func queryRawResponse[ReqDto any](ctx context.Context, rc *RestClient, behalfUserId int64, method, url, opName string, req *ReqDto) (*http.Response, error) {
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

		httpReq.Body = io.NopCloser(reader)
	}

	ctx, span := rc.tracer.Start(ctx, opName)
	defer span.End()
	httpReq = httpReq.WithContext(ctx)

	if rc.cfg.RestClientConfig.Dump {
		dumpReq, err := httputil.DumpRequestOut(httpReq, true)
		if err != nil {
			return nil, err
		}
		fmt.Println("[test http client] >>>")
		fmt.Println(string(dumpReq))
	}

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

	if rc.cfg.RestClientConfig.Dump {
		dumpResp, err := httputil.DumpResponse(httpResp, true)
		if err != nil {
			return nil, err
		}
		fmt.Println("[test http client] <<<")
		fmt.Println(string(dumpResp))
	}
	return httpResp, err
}

func query[ReqDto any, ResDto any](ctx context.Context, rc *RestClient, behalfUserId int64, method, url, opName string, req *ReqDto) (ResDto, error) {
	var resp ResDto
	var err error
	httpResp, err := queryRawResponse(ctx, rc, behalfUserId, method, url, opName, req)
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

func queryNoResponse[ReqDto any](ctx context.Context, rc *RestClient, behalfUserId int64, method, url, opName string, req *ReqDto) error {
	var err error
	httpResp, err := queryRawResponse(ctx, rc, behalfUserId, method, url, opName, req)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	return nil
}
