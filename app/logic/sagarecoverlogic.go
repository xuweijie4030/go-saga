package logic

import (
	"context"
	"github.com/carefreex-io/logger"
	"gosaga/app/common"
	"gosaga/app/dao/db"
	"gosaga/app/logic/commonlogic"
	"gosaga/app/repository"
	"sync"
)

type SagaRecoverLogic struct {
}

var (
	sagaRecoverLogic     *SagaRecoverLogic
	sagaRecoverLogicOnce sync.Once
)

func NewSagaRecoverLogic() *SagaRecoverLogic {
	if sagaRecoverLogic != nil {
		return sagaRecoverLogic
	}
	sagaRecoverLogicOnce.Do(func() {
		sagaRecoverLogic = &SagaRecoverLogic{}
	})

	return sagaRecoverLogic
}

func (l *SagaRecoverLogic) SagaRecoverHandler(ctx context.Context, sagaIds []int) (err error) {
	sagas, err := repository.NewSagaRepository().GetSagaByIds(ctx, sagaIds)
	if err != nil {
		logger.ErrorfX(ctx, "get saga by ids failed: ")
	}
	if len(sagas) == 0 {
		logger.InfofX(ctx, "not found any saga")
		return nil
	}

	sagaLogs, err := repository.NewSagaLogRepository().GetSagaLogBySagaIds(ctx, sagaIds)
	if err != nil {
		logger.ErrorfX(ctx, "get saga log by ids failed: sagaIds=%v err=%v", sagaIds, err)
		return err
	}

	sagaLogMap := commonlogic.NewSagaLogCommonLogic().GetSagaLogMap(sagaLogs)
	for _, saga := range sagas {
		ctx = context.WithValue(ctx, common.SagaTraceIdKey, saga.TraceId)
		go commonlogic.NewSagaRecoverCommonLogic().Recover(ctx, saga, sagaLogMap[saga.Id])
	}

	return nil
}

func (l *SagaRecoverLogic) GetWaitRecoverSagaInfo(ctx context.Context) (sagas []db.Saga, sagaLogs []db.SagaLog, err error) {
	if sagas, err = repository.NewSagaRepository().GetWaitRecoverSaga(ctx); err != nil {
		logger.ErrorfX(ctx, "get wait recover saga failed: err=%v", err)
		return sagas, sagaLogs, err
	}
	if len(sagas) == 0 {
		return sagas, sagaLogs, nil
	}
	sagaIds := make([]int, len(sagas))
	for i, saga := range sagas {
		sagaIds[i] = saga.Id
	}
	if sagaLogs, err = repository.NewSagaLogRepository().GetSagaLogBySagaIds(ctx, sagaIds); err != nil {
		logger.ErrorfX(ctx, "get saga log by ids failed: sagaIds=%v err=%v", sagaIds, err)
		return sagas, sagaLogs, err
	}

	return sagas, sagaLogs, nil
}
