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

type CompensateApi struct {
}

var (
	compensateApi     *CompensateApi
	compensateApiOnce sync.Once
)

func NewCompensateApi() *CompensateApi {
	if compensateApi != nil {
		return compensateApi
	}
	compensateApiOnce.Do(func() {
		compensateApi = &CompensateApi{}
	})

	return compensateApi
}

func (a *CompensateApi) Call(ctx context.Context, url string, headers map[string]string, request proto.CompensateRequest, timeout time.Duration) (response proto.CompensateResponse, err error) {
	logger.InfofX(ctx, "call compensate info: url=%v headers=%v request=%v", url, headers, request)

	responseByte, err := util.Post(ctx, url, request, headers, timeout)

	if err != nil {
		logger.ErrorfX(ctx, "api call failed: url=%v headers=%v request=%v err=%v", url, headers, request, err)
		response = proto.CompensateResponse{
			Msg:    err.Error(),
			Status: gosaga.CompensateFailed,
		}
		return response, err
	}

	if err = json.Unmarshal(responseByte, &response); err != nil {
		logger.ErrorfX(ctx, "json.Unmarshal failed: params=%v err=%v", string(responseByte), err)
		response = proto.CompensateResponse{
			Msg:    err.Error(),
			Status: gosaga.CompensateFailed,
		}
		return response, err
	}

	response = proto.CompensateResponse{
		Msg:    response.Msg,
		Status: response.Status,
	}

	return response, nil
}
