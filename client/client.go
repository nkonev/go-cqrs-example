package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
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
	contentType := "application/json;charset=UTF-8"
	fullUrl := utils.StringToUrl("http://localhost" + rc.cfg.HttpServerConfig.Address + "/chat")

	requestHeaders := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept":          {contentType},
		"Content-Type":    {contentType},
		"X-UserId":        {utils.ToString(behalfUserId)},
	}

	req := handlers.ChatCreateDto{
		Title: chatName,
	}

	bytesData, err := json.Marshal(req)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error("Failed during marshalling:", "err", err)
		return 0, err
	}
	reader := bytes.NewReader(bytesData)

	nopCloser := ioutil.NopCloser(reader)

	request := &http.Request{
		Method: "POST",
		Header: requestHeaders,
		URL:    fullUrl,
		Body:   nopCloser,
	}

	ctx, span := rc.tracer.Start(ctx, "chat.Create")
	defer span.End()
	request = request.WithContext(ctx)
	resp, err := rc.Do(request)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to request get create chat response:", "err", err)
		return 0, err
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	if code != 200 {
		logger.LogWithTrace(ctx, rc.lgr).Warn("create chat response responded non-200 code: ", "code", code)
		return 0, errors.New("create chat response responded non-200 code")
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to decode get create chat response:", "err", err)
		return 0, err
	}

	respDto := handlers.IdResponse{}
	if err = json.Unmarshal(bodyBytes, &respDto); err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error("Failed to parse create chat response:", "err", err)
		return 0, err
	}
	return respDto.Id, nil
}

func (rc *RestClient) GetChatsByUserId(ctx context.Context, behalfUserId int64) ([]cqrs.ChatViewDto, error) {
	contentType := "application/json;charset=UTF-8"
	fullUrl := utils.StringToUrl("http://localhost" + rc.cfg.HttpServerConfig.Address + "/chat/search")

	requestHeaders := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept":          {contentType},
		"Content-Type":    {contentType},
		"X-UserId":        {utils.ToString(behalfUserId)},
	}

	request := &http.Request{
		Method: "GET",
		Header: requestHeaders,
		URL:    fullUrl,
	}

	respDto := []cqrs.ChatViewDto{}

	ctx, span := rc.tracer.Start(ctx, "chat.Search")
	defer span.End()
	request = request.WithContext(ctx)
	resp, err := rc.Do(request)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to request get get chat response:", "err", err)
		return respDto, err
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	if code != 200 {
		logger.LogWithTrace(ctx, rc.lgr).Warn("get chat response responded non-200 code: ", "code", code)
		return respDto, errors.New("get chat response responded non-200 code")
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to decode get get chat response:", "err", err)
		return respDto, err
	}

	if err = json.Unmarshal(bodyBytes, &respDto); err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error("Failed to parse get chat response:", "err", err)
		return respDto, err
	}
	return respDto, nil
}

func (rc *RestClient) CreateMessage(ctx context.Context, behalfUserId int64, chatId int64, text string) (int64, error) {
	contentType := "application/json;charset=UTF-8"
	fullUrl := utils.StringToUrl("http://localhost" + rc.cfg.HttpServerConfig.Address + "/chat/" + utils.ToString(chatId) + "/message")

	requestHeaders := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept":          {contentType},
		"Content-Type":    {contentType},
		"X-UserId":        {utils.ToString(behalfUserId)},
	}

	req := handlers.MessageCreateDto{
		Content: text,
	}

	bytesData, err := json.Marshal(req)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error("Failed during marshalling:", "err", err)
		return 0, err
	}
	reader := bytes.NewReader(bytesData)

	nopCloser := ioutil.NopCloser(reader)

	request := &http.Request{
		Method: "POST",
		Header: requestHeaders,
		URL:    fullUrl,
		Body:   nopCloser,
	}

	ctx, span := rc.tracer.Start(ctx, "message.Create")
	defer span.End()
	request = request.WithContext(ctx)
	resp, err := rc.Do(request)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to request get create message response:", "err", err)
		return 0, err
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	if code != 200 {
		logger.LogWithTrace(ctx, rc.lgr).Warn("create message response responded non-200 code: ", "code", code)
		return 0, errors.New("create message response responded non-200 code")
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to decode get create message response:", "err", err)
		return 0, err
	}

	idResp := handlers.IdResponse{}
	if err = json.Unmarshal(bodyBytes, &idResp); err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error("Failed to parse create message response:", "err", err)
		return 0, err
	}
	return idResp.Id, nil
}

func (rc *RestClient) GetMessages(ctx context.Context, behalfUserId int64, chatId int64) ([]cqrs.MessageViewDto, error) {
	contentType := "application/json;charset=UTF-8"
	fullUrl := utils.StringToUrl("http://localhost" + rc.cfg.HttpServerConfig.Address + "/chat/" + utils.ToString(chatId) + "/message/search")

	requestHeaders := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept":          {contentType},
		"Content-Type":    {contentType},
		"X-UserId":        {utils.ToString(behalfUserId)},
	}

	request := &http.Request{
		Method: "GET",
		Header: requestHeaders,
		URL:    fullUrl,
	}

	var respDto = []cqrs.MessageViewDto{}

	ctx, span := rc.tracer.Start(ctx, "message.Search")
	defer span.End()
	request = request.WithContext(ctx)
	resp, err := rc.Do(request)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to request get messages response:", "err", err)
		return respDto, err
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	if code != 200 {
		logger.LogWithTrace(ctx, rc.lgr).Warn("get messages response responded non-200 code: ", "code", code)
		return respDto, errors.New("get messages response responded non-200 code")
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Warn("Failed to decode get get messages response:", "err", err)
		return respDto, err
	}

	if err = json.Unmarshal(bodyBytes, &respDto); err != nil {
		logger.LogWithTrace(ctx, rc.lgr).Error("Failed to parse get messages response:", "err", err)
		return respDto, err
	}
	return respDto, nil
}
