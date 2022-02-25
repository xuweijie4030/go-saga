package commonlogic

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"gosaga/app/repository"
	"sync"
)

type SagaExecCommonLogic struct {
}

var (
	sagaExecCommonLogic     *SagaExecCommonLogic
	sagaExecCommonLogicOnce sync.Once
)

func NewSagaExecCommonLogic() *SagaExecCommonLogic {
	if sagaExecCommonLogic == nil {
		sagaExecCommonLogicOnce.Do(func() {
			sagaExecCommonLogic = &SagaExecCommonLogic{}
		})
	}

	return sagaExecCommonLogic
}

func (l *SagaExecCommonLogic) RecordRunResult(ctx context.Context, sagaId int, lastIndex int, result bool) (err error) {
	if !result {
		if err = repository.NewSagaRepository().PendingSaga(ctx, sagaId); err != nil {
			logger.ErrorfX(ctx, "pending saga failed: sagaId=%v err=%v", sagaId, err)
			return err
		}
		return nil
	}

	if err = repository.NewSagaLogRepository().RecordSagaEnded(ctx, sagaId, lastIndex); err != nil {
		logger.ErrorfX(ctx, "record saga ended failed: sagaId=%v err=%v", sagaId, err)
		return err
	}

	if err = repository.NewSagaRepository().FinishSaga(ctx, sagaId); err != nil {
		logger.ErrorfX(ctx, "finished saga failed: sagaId=%v err=%v", sagaId, err)
		return err
	}

	return nil
}

func (l *SagaExecCommonLogic) FinishSaga(ctx context.Context, sagaId int) (err error) {
	if err = repository.NewSagaRepository().FinishSaga(ctx, sagaId); err != nil {
		logger.ErrorfX(ctx, "finished saga failed: sagaId=%v err=%v", sagaId, err)
		return err
	}

	return nil
}

func (l *SagaExecCommonLogic) GetCompensateType(globalCompensateType int, transactionCompensateType int) int {
	if globalCompensateType == 0 {
		return transactionCompensateType
	}

	return globalCompensateType
}

func (l *SagaExecCommonLogic) DoSaga(ctx context.Context, sagaId int, spanIds []string, sagaInfo proto.SagaInfo) (result bool) {
	result = false
	if sagaInfo.ExecType == gosaga.SerialExecType {
		result = l.DoSerialSaga(ctx, sagaId, spanIds, sagaInfo, 0)
	} else {
		result = l.DoConcurrentSaga(ctx, sagaId, spanIds, sagaInfo, map[int]byte{})
	}

	return result
}

func (l *SagaExecCommonLogic) DoSerialSaga(ctx context.Context, sagaId int, spanIds []string, sagaInfo proto.SagaInfo, startIndex int) bool {
	failedIndex := l.doSerialTransaction(ctx, sagaId, spanIds, sagaInfo.Transaction, startIndex)

	if failedIndex < 0 {
		return true
	}

	if failedIndex == 0 || l.GetCompensateType(sagaInfo.GlobalCompensateType, sagaInfo.Transaction[failedIndex].CompensateType) == gosaga.ForwardCompensateType {
		return false
	}

	return l.DoSerialCompensate(ctx, sagaId, failedIndex-1, spanIds, sagaInfo.Transaction)
}

func (l *SagaExecCommonLogic) doSerialTransaction(ctx context.Context, sagaId int, spanIds []string, transactions []proto.Transaction, startIndex int) (failedIndex int) {
	failedIndex = -1
	previousResult := make([]map[string]interface{}, 0)

	for i := startIndex; i < len(transactions); i++ {
		ctx = context.WithValue(ctx, common.SagaSpanIdKey, spanIds[i])

		transaction := transactions[i]
		transactionIndex := i + 1
		if err := repository.NewSagaLogRepository().RecordTransactionBegin(ctx, sagaId, transactionIndex, transaction); err != nil {
			logger.ErrorfX(ctx, "record transaction begin failed: transaction=%v", transaction)
			return failedIndex
		}

		response := repository.NewTransactionRepository().CallTransaction(ctx, transaction, previousResult)

		if response[len(response)-1].Status != gosaga.TransactionSuccess {
			if err := repository.NewSagaLogRepository().RecordTransactionAborted(ctx, sagaId, transactionIndex, response); err != nil {
				logger.ErrorfX(ctx, "record transaction aborted failed: transaction=%v", transaction)
			}
			failedIndex = i
			break
		}

		previousResult = append(previousResult, response[len(response)-1].Data)
		if err := repository.NewSagaLogRepository().RecordTransactionEnd(ctx, sagaId, transactionIndex, response); err != nil {
			logger.ErrorfX(ctx, "record transaction end failed: transaction=%v", transaction)
			return failedIndex
		}
	}

	return failedIndex
}

func (l *SagaExecCommonLogic) DoSerialCompensate(ctx context.Context, sagaId int, startCompensateIndex int, spanIds []string, transactions []proto.Transaction) bool {
	for startCompensateIndex >= 0 {
		ctx = context.WithValue(ctx, common.SagaSpanIdKey, spanIds[startCompensateIndex])
		transactionIndex := startCompensateIndex + 1
		transaction := transactions[startCompensateIndex]

		responses := repository.NewCompensateRepository().CallCompensate(ctx, transaction)

		if responses[len(responses)-1].Status == gosaga.CompensateFailed {
			return false
		}

		if err := repository.NewSagaLogRepository().RecordTransactionCompensated(ctx, sagaId, transactionIndex, responses); err != nil {
			logger.ErrorfX(ctx, "record transaction compensate failed: sagaId=%v transactionIndex transaction=%v", sagaId, transactionIndex, transaction)
			return false
		}

		startCompensateIndex--
	}

	return true
}

func (l *SagaExecCommonLogic) DoConcurrentSaga(ctx context.Context, sagaId int, spanIds []string, sagaInfo proto.SagaInfo, executedIndex map[int]byte) bool {
	if err := repository.NewSagaLogRepository().MultipleRecordTransactionBegin(ctx, sagaId, spanIds, sagaInfo.Transaction); err != nil {
		logger.ErrorfX(ctx, "record transaction begin failed: sagaId=%v spanIds=%v sagaInfo=%v", sagaId, spanIds, sagaInfo.Transaction)
		return false
	}

	if l.doConcurrentTransaction(ctx, sagaId, spanIds, sagaInfo.Transaction, executedIndex) {
		return true
	}

	if sagaInfo.GlobalCompensateType == gosaga.ForwardCompensateType {
		return false
	}

	return l.DoConcurrentCompensate(ctx, sagaId, spanIds, sagaInfo.Transaction, map[int]byte{})
}

func (l *SagaExecCommonLogic) doConcurrentTransaction(ctx context.Context, sagaId int, spanIds []string, transactions []proto.Transaction, executedIndex map[int]byte) bool {
	transactionChs := make([]chan bool, 0)

	for i, transaction := range transactions {
		ch := make(chan bool)
		transactionChs = append(transactionChs, ch)
		transactionIndex := i + 1

		go func(ctx context.Context, transaction proto.Transaction) {
			if _, ok := executedIndex[transactionIndex-1]; ok {
				ch <- true
				return
			}
			ctx = context.WithValue(ctx, common.SagaSpanIdKey, spanIds[transactionIndex-1])

			response := repository.NewTransactionRepository().CallTransaction(ctx, transaction, []map[string]interface{}{})

			if response[len(response)-1].Status != gosaga.TransactionSuccess {
				if err := repository.NewSagaLogRepository().RecordTransactionAborted(ctx, sagaId, transactionIndex, response); err != nil {
					logger.ErrorfX(ctx, "record transaction aborted failed: transaction=%v", transaction)
				}
				ch <- false
				return
			}
			if err := repository.NewSagaLogRepository().RecordTransactionEnd(ctx, sagaId, transactionIndex, response); err != nil {
				logger.ErrorfX(ctx, "record transaction end failed: transaction=%v", transaction)
				ch <- false
				return
			}
			ch <- true
		}(ctx, transaction)
	}

	isSuccess := true
	for _, ch := range transactionChs {
		if !<-ch {
			isSuccess = false
			break
		}
	}

	return isSuccess
}

func (l *SagaExecCommonLogic) DoConcurrentCompensate(ctx context.Context, sagaId int, spanIds []string, transactions []proto.Transaction, executedIndex map[int]byte) bool {
	compensateChs := make([]chan bool, 0)
	for i, transaction := range transactions {
		ch := make(chan bool)
		compensateChs = append(compensateChs, ch)
		transactionIndex := i + 1

		go func(ctx context.Context, transaction proto.Transaction) {
			if _, ok := executedIndex[transactionIndex-1]; ok {
				ch <- true
				return
			}
			ctx = context.WithValue(ctx, common.SagaSpanIdKey, spanIds[transactionIndex-1])

			responses := repository.NewCompensateRepository().CallCompensate(ctx, transaction)

			if responses[len(responses)-1].Status == gosaga.CompensateFailed {
				ch <- false
				return
			}

			if err := repository.NewSagaLogRepository().RecordTransactionCompensated(ctx, sagaId, transactionIndex, responses); err != nil {
				logger.ErrorfX(ctx, "record transaction compensate failed: sagaId=%v transactionIndex transaction=%v", sagaId, transactionIndex, transaction)
				ch <- false
				return
			}
			ch <- true
		}(ctx, transaction)
	}

	isSuccess := true
	for _, ch := range compensateChs {
		if !<-ch {
			isSuccess = false
		}
	}

	return isSuccess
}
