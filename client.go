package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"io/ioutil"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RestClient struct {
	*http.Client
	tracer        trace.Tracer
	lgr           *slog.Logger
	baseUrl       string
	getOnlinesUrl string
}

type UserOnline struct {
	Id     int64 `json:"userId"`
	Online bool  `json:"online"`
}

func NewRestClient(lgr *slog.Logger, baseUrl, getOnlinesUrl string) *RestClient {
	tr := &http.Transport{
		MaxIdleConns:       2,
		IdleConnTimeout:    time.Second * 10,
		DisableCompression: false,
	}
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	trR := otelhttp.NewTransport(tr)
	client := &http.Client{Transport: trR}
	trcr := otel.Tracer("rest/client")

	return &RestClient{client, trcr, lgr, baseUrl, getOnlinesUrl}
}

func (rc RestClient) GetOnlines(c context.Context, userIds []int64) ([]*UserOnline, error) {
	contentType := "application/json;charset=UTF-8"
	url0 := rc.baseUrl
	url1 := rc.getOnlinesUrl
	fullUrl := url0 + url1

	var userIdsString []string
	for _, userIdInt := range userIds {
		userIdsString = append(userIdsString, ToString(userIdInt))
	}

	join := strings.Join(userIdsString, ",")

	requestHeaders := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept":          {contentType},
		"Content-Type":    {contentType},
	}

	parsedUrl, err := url.Parse(fullUrl + "?userId=" + join)
	if err != nil {
		LogWithTrace(c, rc.lgr).Error("Failed during parse aaa url:", "err", err)
		return nil, err
	}
	request := &http.Request{
		Method: "GET",
		Header: requestHeaders,
		URL:    parsedUrl,
	}

	ctx, span := rc.tracer.Start(c, "users.Onlines")
	defer span.End()
	request = request.WithContext(ctx)
	resp, err := rc.Do(request)
	if err != nil {
		LogWithTrace(c, rc.lgr).Warn("Failed to request get users response:", "err", err)
		return nil, err
	}
	defer resp.Body.Close()
	code := resp.StatusCode
	if code != 200 {
		LogWithTrace(c, rc.lgr).Warn("Users response responded non-200 code: ", "code", code)
		return nil, errors.New("Users response responded non-200 code")
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		LogWithTrace(c, rc.lgr).Error("Failed to decode get users response:", "err", err)
		return nil, err
	}

	users := &[]*UserOnline{}
	if err := json.Unmarshal(bodyBytes, users); err != nil {
		LogWithTrace(c, rc.lgr).Error("Failed to parse users:", "err", err)
		return nil, err
	}
	return *users, nil
}

func (rc RestClient) IsOnline(c context.Context, userId int64) (bool, error) {
	onlines, err := rc.GetOnlines(c, []int64{userId})
	if err != nil {
		return false, err
	}

	if len(onlines) != 1 {
		return false, errors.New("Not 1 online")
	}

	return onlines[0].Online, nil
}
