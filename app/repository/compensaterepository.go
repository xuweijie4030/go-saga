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

type CompensateRepository struct {
}

var (
	compensateRepository     *CompensateRepository
	compensateRepositoryOnce sync.Once
)

func NewCompensateRepository() *CompensateRepository {
	if compensateRepository != nil {
		return compensateRepository
	}
	compensateRepositoryOnce.Do(func() {
		compensateRepository = &CompensateRepository{}
	})

	return compensateRepository
}

func (r *CompensateRepository) CallCompensate(ctx context.Context, transaction proto.Transaction) (result []proto.CompensateResponse) {
	if transaction.CallType == gosaga.ApiTransaction {
		result = r.callApiCompensate(ctx, transaction)
	} else {
		result = r.callRpcCompensate(ctx, transaction)
	}

	return result
}

func (r *CompensateRepository) callApiCompensate(ctx context.Context, transaction proto.Transaction) (result []proto.CompensateResponse) {
	result = make([]proto.CompensateResponse, 0)

	request := proto.CompensateRequest{
		TraceId: common.GetTraceId(ctx),
		SpanId:  common.GetSpanId(ctx),
		Params:  transaction.Data,
	}
	logger.InfofX(ctx, "call api compensate request=%v", request)

	for i := 1; i <= transaction.MaxRetryTimes; i++ {
		response, err := api.NewCompensateApi().Call(ctx, transaction.Compensate, transaction.Headers, request, transaction.TimeoutToFail)
		if err != nil {
			logger.ErrorfX(ctx, "call api compensate failed: transaction=%v err=%v", transaction, err)
			result = append(result, response)

			time.Sleep(transaction.RetryInterval * time.Second)
			continue
		}
		if response.Status == gosaga.CompensateFailed {
			result = append(result, response)
			logger.ErrorfX(ctx, "call api compensate failed: transaction=%v response=%v err=the called server response failed", transaction, response)
			time.Sleep(transaction.RetryInterval * time.Second)
			continue
		}
		result = append(result, response)
		break
	}

	return result
}

func (r *CompensateRepository) callRpcCompensate(ctx context.Context, transaction proto.Transaction) (result []proto.CompensateResponse) {
	result = make([]proto.CompensateResponse, 0)

	for i := 1; i <= transaction.MaxRetryTimes; i++ {
		response, err := rpc.NewCompensateRpc().Call(ctx, transaction)
		if err != nil {
			logger.ErrorfX(ctx, "call rpc compensate failed: transaction=%v err=%v", transaction, err)
			result = append(result, response)

			time.Sleep(transaction.RetryInterval * time.Second)
			continue
		}
		if response.Status == gosaga.CompensateFailed {
			result = append(result, response)
			logger.ErrorfX(ctx, "call rpc compensate failed: transaction=%v response=%v err=the called server response failed", transaction, response)
			time.Sleep(transaction.RetryInterval * time.Second)
			continue
		}
		result = append(result, response)
		break
	}

	return result
}
