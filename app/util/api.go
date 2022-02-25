package util

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/carefreex-io/logger"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

var client = &http.Client{}

func Post(ctx context.Context, url string, data interface{}, headers map[string]string, timeout time.Duration) (result []byte, err error) {
	logger.InfofX(ctx, "post request: url=%v data=%v headers=%v", url, data, headers)

	requestData, err := json.Marshal(data)
	if err != nil {
		logger.ErrorfX(ctx, "json.Marshal failed: data=%v err=%v", data, err)
		return result, err
	}

	request, err := http.NewRequest("POST", url, bytes.NewReader(requestData))
	if err != nil {
		logger.ErrorfX(ctx, "http.NewRequest failed: url=%v requestData=%v", url, string(requestData))
		return result, err
	}
	if len(headers) != 0 {
		for key, val := range headers {
			request.Header.Add(key, val)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout*time.Second)
	defer cancel()
	request.WithContext(ctx)

	response, err := client.Do(request)
	if err != nil {
		logger.ErrorfX(ctx, "client.Post failed: url=%v request=%v err=%v", url, string(requestData), err)
		return result, err
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
	}(response.Body)

	result, err = ioutil.ReadAll(response.Body)
	if err != nil {
		logger.ErrorfX(ctx, "ioutil.ReadAll failed: response=%v err=%v", response, err)
		return result, err
	}

	return result, nil
}
