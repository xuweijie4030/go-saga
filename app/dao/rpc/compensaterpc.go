package rpc

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/carefreex-io/rpcxclient"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"sync"
)

type CompensateRpc struct {
}

var (
	compensateRpc     *CompensateRpc
	compensateRpcOnce sync.Once
)

func NewCompensateRpc() *CompensateRpc {
	if compensateRpc != nil {
		return compensateRpc
	}
	compensateRpcOnce.Do(func() {
		compensateRpc = &CompensateRpc{}
	})

	return compensateRpc
}

func (r *CompensateRpc) Call(ctx context.Context, transaction proto.Transaction) (response proto.CompensateResponse, err error) {
	logger.InfofX(ctx, "call info: transaction=%v", transaction)

	rpcXClient, err := rpcxclient.NewClient(getOptions(transaction.BasePath, transaction.ServerName, transaction.TimeoutToFail))
	if err != nil {
		logger.ErrorfX(ctx, "new rpcX client failed: transaction=%v err=%v", transaction, err)
		return response, err
	}

	request := proto.CompensateRequest{
		TraceId: common.GetTraceId(ctx),
		SpanId:  common.GetSpanId(ctx),
		Params:  transaction.Data,
	}
	response = proto.CompensateResponse{}
	if err = rpcXClient.Call(ctx, transaction.Compensate, &request, &response); err != nil {
		logger.ErrorfX(ctx, "rpc call failed: request=%v err=%v", request, err)
		return response, err
	}

	return response, nil
}
