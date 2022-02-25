package api

import (
	"context"
	"encoding/json"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/util"
	"sync"
	"time"
)

type TransactionApi struct {
}

var (
	transactionApi     *TransactionApi
	transactionApiOnce sync.Once
)

func NewTransactionApi() *TransactionApi {
	if transactionApi == nil {
		transactionApiOnce.Do(func() {
			transactionApi = &TransactionApi{}
		})
	}

	return transactionApi
}

func (a *TransactionApi) Call(ctx context.Context, url string, headers map[string]string, request proto.TransactionRequest, timeout time.Duration) (response proto.TransactionResponse, err error) {
	logger.InfofX(ctx, "call info: url=%v headers=%v request=%v", url, headers, request)

	responseByte, err := util.Post(ctx, url, request, headers, timeout)

	if err != nil {
		logger.ErrorfX(ctx, "api call failed: url=%v headers=%v request=%v err=%v", url, headers, request, err)
		response = proto.TransactionResponse{
			Msg:    err.Error(),
			Status: gosaga.TransactionFailed,
		}
		return response, err
	}

	if err = json.Unmarshal(responseByte, &response); err != nil {
		logger.ErrorfX(ctx, "json.Unmarshal failed: params=%v err=%v", string(responseByte), err)
		response = proto.TransactionResponse{
			Msg:    err.Error(),
			Status: gosaga.TransactionFailed,
		}
		return response, err
	}

	response = proto.TransactionResponse{
		Msg:    response.Msg,
		Status: response.Status,
		Data:   response.Data,
	}

	return response, nil
}
