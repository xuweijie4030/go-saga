package repository

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"gosaga/app/dao/api"
	"gosaga/app/dao/rpc"
	"sync"
	"time"
)

type TransactionRepository struct {
}

var (
	transactionRepository     *TransactionRepository
	transactionRepositoryOnce sync.Once
)

func NewTransactionRepository() *TransactionRepository {
	if transactionRepository != nil {
		return transactionRepository
	}
	transactionRepositoryOnce.Do(func() {
		transactionRepository = &TransactionRepository{}
	})

	return transactionRepository
}

func (r *TransactionRepository) CallTransaction(ctx context.Context, transaction proto.Transaction, previousResult []map[string]interface{}) (result []proto.TransactionResponse) {
	if transaction.CallType == gosaga.ApiTransaction {
		result = r.CallApiTransaction(ctx, transaction, previousResult)
	} else {
		result = r.CallRpcTransaction(ctx, transaction, previousResult)
	}

	return result
}

func (r *TransactionRepository) CallApiTransaction(ctx context.Context, transaction proto.Transaction, previousResult []map[string]interface{}) (result []proto.TransactionResponse) {
	result = make([]proto.TransactionResponse, 0)

	request := proto.TransactionRequest{
		TraceId:        common.GetTraceId(ctx),
		SpanId:         common.GetSpanId(ctx),
		Params:         transaction.Data,
		PreviousResult: previousResult,
	}
	logger.InfofX(ctx, "call api transaction request=%v", request)
	for i := 1; i <= transaction.MaxRetryTimes; i++ {
		response, err := api.NewTransactionApi().Call(ctx, transaction.Action, transaction.Headers, request, transaction.TimeoutToFail)
		if err != nil {
			logger.ErrorfX(ctx, "call api transaction failed: transaction=%v err=%v", transaction, err)
			result = append(result, response)

			time.Sleep(transaction.RetryInterval * time.Second)
			continue
		}
		result = append(result, response)
		break
	}

	return result
}

func (r *TransactionRepository) CallRpcTransaction(ctx context.Context, transaction proto.Transaction, previousResult []map[string]interface{}) (result []proto.TransactionResponse) {
	result = make([]proto.TransactionResponse, 0)

	for i := 1; i <= transaction.MaxRetryTimes; i++ {
		response, err := rpc.NewTransactionRpc().Call(ctx, transaction, previousResult)
		if err != nil {
			logger.ErrorfX(ctx, "call rpc transaction failed: transaction=%v err=%v", transaction, err)
			result = append(result, response)

			time.Sleep(transaction.RetryInterval * time.Second)
			continue
		}
		result = append(result, response)
		break
	}

	return result
}
