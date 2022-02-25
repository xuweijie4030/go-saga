package service

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/logic"
)

type SagaCommandService struct {
}

func NewSagaCommandService() *SagaCommandService {
	return &SagaCommandService{}
}

func (s *SagaCommandService) SubmitSaga(ctx context.Context, request *proto.SubmitSagaRequest, response *proto.SubmitSagaResponse) (err error) {
	logger.InfofX(ctx, "request=%v", *request)

	param := *request

	for i, transaction := range param.Transaction {
		if transaction.TimeoutToFail == 0 {
			transaction.TimeoutToFail = gosaga.DefaultTimeoutToFail
		}
		if transaction.RetryInterval == 0 {
			transaction.RetryInterval = gosaga.DefaultRetryInterval
		}
		if transaction.MaxRetryTimes == 0 {
			transaction.MaxRetryTimes = gosaga.DefaultMaxRetryTimes
		}
		// 如果没有配置全局事务补偿类型且事务内部也没有配置补偿类型则将当前事务的补偿类型设置为默认补偿类型（默认为向后补偿）
		if param.GlobalCompensateType == gosaga.DefaultGlobalCompensateType && transaction.CompensateType == 0 {
			transaction.CompensateType = gosaga.DefaultTransactionCompensateType
		}
		param.Transaction[i] = transaction
	}

	if response.TraceId, err = logic.NewSagaExecLogic().SagaExecHandler(ctx, param); err != nil {
		logger.ErrorfX(ctx, "submit saga failed: request=%v err=%v", param, err)
		return err
	}

	return nil
}

func (s SagaCommandService) RecoverSaga(ctx context.Context, request *proto.RecoverSagaRequest, response *proto.RecoverSagaResponse) (err error) {
	logger.InfofX(ctx, "request=%v", *request)

	if err = logic.NewSagaRecoverLogic().SagaRecoverHandler(ctx, request.SagaId); err != nil {
		logger.ErrorfX(ctx, "saga recover failed: request=%v err=%v", request, err)
		return err
	}

	return nil
}
