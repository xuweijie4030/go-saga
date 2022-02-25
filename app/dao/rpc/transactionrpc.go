package rpc

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/carefreex-io/rpcxclient"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"sync"
)

type TransactionRpc struct {
}

var (
	transactionRpc     *TransactionRpc
	transactionRpcOnce sync.Once
)

func NewTransactionRpc() *TransactionRpc {
	if transactionRpc == nil {
		transactionRpcOnce.Do(func() {
			transactionRpc = &TransactionRpc{}
		})
	}

	return transactionRpc
}

func (r *TransactionRpc) Call(ctx context.Context, transaction proto.Transaction, previousResult []map[string]interface{}) (response proto.TransactionResponse, err error) {
	logger.InfofX(ctx, "call info: transaction=%v", transaction)

	rpcXClient, err := rpcxclient.NewClient(getOptions(transaction.BasePath, transaction.ServerName, transaction.TimeoutToFail))
	if err != nil {
		logger.ErrorfX(ctx, "new rpcX client failed: transaction=%v err=%v", transaction, err)
		return response, err
	}

	request := proto.TransactionRequest{
		TraceId:        common.GetTraceId(ctx),
		SpanId:         common.GetSpanId(ctx),
		Params:         transaction.Data,
		PreviousResult: previousResult,
	}
	response = proto.TransactionResponse{}
	if err = rpcXClient.Call(ctx, transaction.Action, &request, &response); err != nil {
		logger.ErrorfX(ctx, "rpc call failed: request=%v err=%v", request, err)
		return response, err
	}

	return response, nil
}
