package logic

import (
	"context"
	"github.com/carefreex-io/logger"
	uuid "github.com/satori/go.uuid"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"gosaga/app/logic/commonlogic"
	"gosaga/app/repository"
	"sync"
)

type SagaExecLogic struct {
}

var (
	sagaExecLogic     *SagaExecLogic
	sagaExecLogicOnce sync.Once
)

func NewSagaExecLogic() *SagaExecLogic {
	if sagaExecLogic == nil {
		sagaExecLogicOnce.Do(func() {
			sagaExecLogic = &SagaExecLogic{}
		})
	}

	return sagaExecLogic
}

func (l *SagaExecLogic) SagaExecHandler(ctx context.Context, request proto.SubmitSagaRequest) (traceId string, err error) {
	traceId = uuid.NewV4().String()
	ctx = context.WithValue(ctx, common.SagaTraceIdKey, traceId)

	sagaId, err := repository.NewSagaRepository().CreateSaga(ctx, request)
	if err != nil {
		logger.ErrorfX(ctx, "create saga failed: request=%v err=%v", request, err)
		return traceId, err
	}

	go l.run(ctx, sagaId, request)

	return traceId, nil
}

func (l *SagaExecLogic) run(ctx context.Context, sagaId int, sagaInfo proto.SagaInfo) {
	repository.NewSagaRepository().AddToContainer(sagaId)
	defer repository.NewSagaRepository().RemoveFromContainer(sagaId)

	if err := repository.NewSagaLogRepository().RecordSagaCreated(ctx, sagaId); err != nil {
		logger.ErrorfX(ctx, "record saga created failed: sagaId=%v sagaInfo=%v err=%v", sagaId, sagaInfo, err)
		return
	}

	spanIds := make([]string, 0)
	for i := 0; i < len(sagaInfo.Transaction); i++ {
		spanIds = append(spanIds, uuid.NewV4().String())
	}

	result := commonlogic.NewSagaExecCommonLogic().DoSaga(ctx, sagaId, spanIds, sagaInfo)

	if err := commonlogic.NewSagaExecCommonLogic().RecordRunResult(ctx, sagaId, len(sagaInfo.Transaction)+1, result); err != nil {
		logger.ErrorfX(ctx, "record run result failed: sagaId=%v, err=%v", sagaId, err)
		return
	}

	return
}
