package commonlogic

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/carefreex-io/logger"
	uuid "github.com/satori/go.uuid"
	"github.com/xuweijie4030/go-common/gosaga"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"gosaga/app/dao/db"
	"gosaga/app/repository"
	"sync"
)

type SagaRecoverCommonLogic struct {
}

var (
	sagaRecoverCommonLogic     *SagaRecoverCommonLogic
	sagaRecoverCommonLogicOnce sync.Once
)

func NewSagaRecoverCommonLogic() *SagaRecoverCommonLogic {
	if sagaRecoverCommonLogic == nil {
		sagaRecoverCommonLogicOnce.Do(func() {
			sagaRecoverCommonLogic = &SagaRecoverCommonLogic{}
		})
	}

	return sagaRecoverCommonLogic
}

func (l *SagaRecoverCommonLogic) Recover(ctx context.Context, saga db.Saga, sagaLogs []db.SagaLog) {
	repository.NewSagaRepository().AddToContainer(saga.Id)
	defer repository.NewSagaRepository().RemoveFromContainer(saga.Id)

	content := proto.SagaInfo{}
	if jsonErr := json.Unmarshal([]byte(saga.Content), &content); jsonErr != nil {
		logger.ErrorfX(ctx, "json.Unmarshal failed: param=%v err=%v", saga.Content, jsonErr)
		return
	}
	if content.ExecType == gosaga.SerialExecType {
		if err := l.recoverSerialSaga(ctx, saga, content, sagaLogs); err != nil {
			logger.ErrorfX(ctx, "recover serial saga failed: saga=%v content=%v sagaLogs=%v err=%v", saga, content, sagaLogs, err)
			return
		}
	} else {
		if err := l.recoverConcurrentSaga(ctx, saga, content, sagaLogs); err != nil {
			logger.ErrorfX(ctx, "recover concurrent saga failed: saga=%v content=%v sagaLogs=%v err=%v", saga, content, sagaLogs, err)
			return
		}
	}

	return
}

func (l *SagaRecoverCommonLogic) recoverSerialSaga(ctx context.Context, saga db.Saga, sagaContent proto.SagaInfo, sagaLogs []db.SagaLog) (err error) {
	spanIds := l.GetSpanIds(sagaLogs, len(sagaContent.Transaction))
	nextHandleType, index := l.GetSerialSagaNextHandleType(sagaLogs)
	result := false
	switch nextHandleType {
	case common.RecoverTransactionHandleType:
		result = NewSagaExecCommonLogic().DoSerialSaga(ctx, saga.Id, spanIds, sagaContent, index)
		if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
			logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	case common.RecoverCompensateHandleType:
		compensateType := NewSagaExecCommonLogic().GetCompensateType(sagaContent.GlobalCompensateType, sagaContent.Transaction[index].CompensateType)
		if compensateType == gosaga.ForwardCompensateType {
			if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
				logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
				return err
			}
			return nil
		}
		result = NewSagaExecCommonLogic().DoSerialCompensate(ctx, saga.Id, index, spanIds, sagaContent.Transaction)
		if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
			logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	case common.RecordSagaLogEndHandleType:
		result = true
		if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
			logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	case common.RecordSagaEndHandleType:
		if err = NewSagaExecCommonLogic().FinishSaga(ctx, saga.Id); err != nil {
			logger.ErrorfX(ctx, "finish saga failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	default:
		err = errors.New("no handle found to perform")
		logger.ErrorfX(ctx, "recover serial saga failed: saga=%v, sagaContent=%v sagaLogs=%v err=%v", saga, sagaContent, sagaLogs)
		return err
	}

	return nil
}

func (l *SagaRecoverCommonLogic) recoverConcurrentSaga(ctx context.Context, saga db.Saga, sagaContent proto.SagaInfo, sagaLogs []db.SagaLog) (err error) {
	spanIds := l.GetSpanIds(sagaLogs, len(sagaContent.Transaction))
	nextHandleType, executedIndex := l.GetConcurrentSagaNextHandleType(sagaLogs, len(sagaContent.Transaction))

	result := false
	switch nextHandleType {
	case common.RecoverTransactionHandleType:
		result = NewSagaExecCommonLogic().DoConcurrentSaga(ctx, saga.Id, spanIds, sagaContent, executedIndex)
		if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
			logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	case common.RecoverCompensateHandleType:
		if sagaContent.GlobalCompensateType == gosaga.ForwardCompensateType {
			if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
				logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
				return err
			}
			return nil
		}
		result = NewSagaExecCommonLogic().DoConcurrentCompensate(ctx, saga.Id, spanIds, sagaContent.Transaction, executedIndex)
		if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
			logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	case common.RecordSagaLogEndHandleType:
		result = true
		if err = NewSagaExecCommonLogic().RecordRunResult(ctx, saga.Id, len(sagaContent.Transaction)+1, result); err != nil {
			logger.ErrorfX(ctx, "record run result failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	case common.RecordSagaEndHandleType:
		if err = NewSagaExecCommonLogic().FinishSaga(ctx, saga.Id); err != nil {
			logger.ErrorfX(ctx, "finish saga failed: saga=%v, sagaContent=%v, sagaLogs=%v", saga, sagaContent, sagaLogs)
			return err
		}
	default:
		err = errors.New("no handle found to perform")
		logger.ErrorfX(ctx, "recover concurrent saga failed: saga=%v, sagaContent=%v sagaLogs=%v err=%v", saga, sagaContent, sagaLogs)
		return err
	}

	return nil
}

func (l *SagaRecoverCommonLogic) GetSpanIds(sagaLogs []db.SagaLog, totalSpanIdNum int) (result []string) {
	result = make([]string, 0)
	spanIdLog := make(map[string]byte)
	for _, sagaLog := range sagaLogs {
		if sagaLog.SpanId == "" {
			continue
		}
		if _, ok := spanIdLog[sagaLog.SpanId]; ok {
			continue
		}
		result = append(result, sagaLog.SpanId)
		spanIdLog[sagaLog.SpanId] = 0
	}
	lackSpanIdNum := totalSpanIdNum - len(result)
	if lackSpanIdNum == 0 {
		return result
	}
	for i := 0; i < lackSpanIdNum; i++ {
		result = append(result, uuid.NewV4().String())
	}

	return result
}

func (l *SagaRecoverCommonLogic) GetConcurrentSagaNextHandleType(sagaLogs []db.SagaLog, transactionLen int) (handleType int, executedIndex map[int]byte) {
	executedIndex = make(map[int]byte)

	if l.isCreatedInterrupt(sagaLogs) {
		return common.RecoverTransactionHandleType, executedIndex
	}

	lastLog := sagaLogs[len(sagaLogs)-1]
	if l.isRecordSagaEndInterrupt(lastLog) {
		return common.RecordSagaEndHandleType, executedIndex
	}

	eventNum := map[string]int{
		"begin":      0,
		"end":        0,
		"aborted":    0,
		"compensate": 0,
	}
	eventIndex := map[string]map[int]byte{
		"begin":      {},
		"end":        {},
		"aborted":    {},
		"compensate": {},
	}
	for _, sagaLog := range sagaLogs {
		switch sagaLog.Event {
		case common.TransactionBeginEvent:
			eventNum["begin"]++
			eventIndex["begin"][sagaLog.Index] = 0
		case common.TransactionEndEvent:
			eventNum["end"]++
			eventIndex["end"][sagaLog.Index] = 0
		case common.TransactionAbortedEvent:
			eventNum["aborted"]++
			eventIndex["aborted"][sagaLog.Index] = 0
		case common.TransactionCompensateEvent:
			eventNum["compensate"]++
			eventIndex["compensate"][sagaLog.Index] = 0
		}
	}
	if eventNum["begin"] == transactionLen && (eventNum["end"]+eventNum["aborted"]+eventNum["compensate"] == 0) {
		return common.RecoverTransactionHandleType, executedIndex
	}
	if eventNum["compensate"] == transactionLen || eventNum["end"] == transactionLen {
		return common.RecordSagaLogEndHandleType, executedIndex
	}
	if eventNum["compensate"] > 0 {
		for index := range eventIndex["begin"] {
			if _, ok := eventIndex["compensate"][index]; ok {
				executedIndex[index-1] = 0
			}
		}
		return common.RecoverCompensateHandleType, executedIndex
	}
	if eventNum["aborted"] > 0 {
		return common.RecoverCompensateHandleType, executedIndex
	}

	for index := range eventIndex["begin"] {
		if _, ok := eventIndex["end"][index]; ok {
			executedIndex[index-1] = 0
		}
	}
	return common.RecoverTransactionHandleType, executedIndex
}

func (l *SagaRecoverCommonLogic) GetSerialSagaNextHandleType(sagaLogs []db.SagaLog) (handleType int, index int) {
	if l.isCreatedInterrupt(sagaLogs) {
		handleType = common.RecoverTransactionHandleType
		return handleType, 0
	}
	lastLog := sagaLogs[len(sagaLogs)-1]
	if l.isExecTransactionInterrupt(lastLog) {
		handleType = common.RecoverTransactionHandleType
		return handleType, lastLog.Index - 1
	}
	if l.isExecTransactionAbortedInterrupt(lastLog) {
		index = lastLog.Index - 2
		if lastLog.Index == 1 {
			handleType = common.RecordSagaLogEndHandleType
			index = 0
		} else {
			handleType = common.RecoverCompensateHandleType
		}
		return handleType, index
	}
	if l.isExecCompensateInterrupt(lastLog) {
		handleType = common.RecoverCompensateHandleType
		return handleType, lastLog.Index - 1
	}
	if l.isRecordSagaLogEndInterrupt(lastLog) {
		handleType = common.RecordSagaLogEndHandleType
		return handleType, 0
	}
	if l.isRecordSagaEndInterrupt(lastLog) {
		handleType = common.RecordSagaEndHandleType
		return handleType, 0
	}

	return 0, 0
}

func (l *SagaRecoverCommonLogic) isCreatedInterrupt(sagaLogs []db.SagaLog) bool {
	return len(sagaLogs) <= 1
}

func (l *SagaRecoverCommonLogic) isExecTransactionInterrupt(lastLog db.SagaLog) bool {
	return lastLog.Event == common.TransactionBeginEvent
}

func (l *SagaRecoverCommonLogic) isExecTransactionAbortedInterrupt(lastLog db.SagaLog) bool {
	return lastLog.Event == common.TransactionAbortedEvent
}

func (l *SagaRecoverCommonLogic) isExecCompensateInterrupt(lastLog db.SagaLog) bool {
	return lastLog.Event == common.TransactionCompensateEvent && lastLog.Index != 1
}

func (l *SagaRecoverCommonLogic) isRecordSagaLogEndInterrupt(lastLog db.SagaLog) bool {
	return lastLog.Event == common.TransactionCompensateEvent && lastLog.Index == 1
}

func (l *SagaRecoverCommonLogic) isRecordSagaEndInterrupt(lastLog db.SagaLog) bool {
	return lastLog.Event == common.SagaEndEvent
}
