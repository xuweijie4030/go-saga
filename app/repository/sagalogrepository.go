package repository

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"gosaga/app/dao/db"
	"gosaga/app/util"
	"sync"
	"time"
)

type SagaLogRepository struct {
}

var (
	sagaLogRepository     *SagaLogRepository
	sagaLogRepositoryOnce sync.Once
)

func NewSagaLogRepository() *SagaLogRepository {
	if sagaLogRepository == nil {
		sagaLogRepositoryOnce.Do(func() {
			sagaLogRepository = &SagaLogRepository{}
		})
	}

	return sagaLogRepository
}

func (r *SagaLogRepository) RecordSagaCreated(ctx context.Context, sagaId int) error {
	sagaLog := db.SagaLog{
		SagaId:    sagaId,
		Event:     common.SagaCreatedEvent,
		Index:     0,
		CreatedAt: time.Now().Unix(),
	}
	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLog); res.Error != nil {
		logger.ErrorfX(ctx, "record saga created failed: sagaLog=%v err=%v", sagaLog, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) RecordTransactionBegin(ctx context.Context, sagaId int, index int, transaction proto.Transaction) error {
	logs, err := db.NewSagaLogDb(ctx).GetBySpanIds([]string{common.GetSpanId(ctx)})
	if err != nil {
		logger.ErrorfX(ctx, "get saga log by span ids failed: spanId=%v err=%v", common.GetSpanId(ctx), err)
	}
	logger.InfofX(ctx, "existing begin logs=%v", logs)
	if len(logs) != 0 {
		return nil
	}

	content, err := util.JsonMarshalToStr(transaction)
	if err != nil {
		logger.ErrorfX(ctx, "generate content json failed: transaction=%v err=%v", transaction, err)
		return err
	}

	sagaLog := db.SagaLog{
		SagaId:    sagaId,
		SpanId:    common.GetSpanId(ctx),
		Event:     common.TransactionBeginEvent,
		Index:     index,
		Content:   content,
		CreatedAt: time.Now().Unix(),
	}
	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLog); res.Error != nil {
		logger.ErrorfX(ctx, "record transaction begin failed: sagaLog=%v err=%v", sagaLog, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) MultipleRecordTransactionBegin(ctx context.Context, sagaId int, spanIds []string, transactions []proto.Transaction) error {
	logs, err := db.NewSagaLogDb(ctx).GetBySpanIds([]string{common.GetSpanId(ctx)})
	if err != nil {
		logger.ErrorfX(ctx, "get saga log by span ids failed: spanId=%v err=%v", common.GetSpanId(ctx), err)
	}
	logger.InfofX(ctx, "existing begin logs=%v", logs)
	if len(logs) == len(transactions) {
		return nil
	}
	existsLog := make(map[string]byte)
	for _, log := range logs {
		existsLog[log.SpanId] = 0
	}

	sagaLogs := make([]db.SagaLog, 0)
	now := time.Now().Unix()

	for i, transaction := range transactions {
		if _, ok := existsLog[spanIds[i]]; ok {
			continue
		}
		content, err := util.JsonMarshalToStr(transaction)
		if err != nil {
			logger.ErrorfX(ctx, "generate content json failed: transaction=%v err=%v", transaction, err)
			return err
		}
		sagaLogs = append(sagaLogs, db.SagaLog{
			SagaId:    sagaId,
			SpanId:    spanIds[i],
			Event:     common.TransactionBeginEvent,
			Index:     i + 1,
			Content:   content,
			CreatedAt: now,
		})
	}

	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLogs); res.Error != nil {
		logger.ErrorfX(ctx, "multiple record transaction begin failed: sagaLogs=%v err=%v", sagaLogs, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) RecordTransactionEnd(ctx context.Context, sagaId int, index int, response []proto.TransactionResponse) error {
	content, err := util.JsonMarshalToStr(response)
	if err != nil {
		logger.ErrorfX(ctx, "generate content json failed: response=%v err=%v", response, err)
		return err
	}

	sagaLog := db.SagaLog{
		SagaId:    sagaId,
		SpanId:    common.GetSpanId(ctx),
		Event:     common.TransactionEndEvent,
		Index:     index,
		Content:   content,
		CreatedAt: time.Now().Unix(),
	}
	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLog); res.Error != nil {
		logger.ErrorfX(ctx, "record transaction end failed: sagaLog=%v err=%v", sagaLog, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) RecordTransactionAborted(ctx context.Context, sagaId int, index int, response []proto.TransactionResponse) error {
	content, err := util.JsonMarshalToStr(response)
	if err != nil {
		logger.ErrorfX(ctx, "generate content json failed: response=%v err=%v", response, err)
		return err
	}

	sagaLog := db.SagaLog{
		SagaId:    sagaId,
		SpanId:    common.GetSpanId(ctx),
		Event:     common.TransactionAbortedEvent,
		Index:     index,
		Content:   content,
		CreatedAt: time.Now().Unix(),
	}
	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLog); res.Error != nil {
		logger.ErrorfX(ctx, "record transaction aborted failed: sagaLog=%v err=%v", sagaLog, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) RecordTransactionCompensated(ctx context.Context, sagaId int, index int, response []proto.CompensateResponse) error {
	content, err := util.JsonMarshalToStr(response)
	if err != nil {
		logger.ErrorfX(ctx, "generate content json failed: response=%v err=%v", response, err)
		return err
	}

	sagaLog := db.SagaLog{
		SagaId:    sagaId,
		SpanId:    common.GetSpanId(ctx),
		Event:     common.TransactionCompensateEvent,
		Index:     index,
		Content:   content,
		CreatedAt: time.Now().Unix(),
	}
	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLog); res.Error != nil {
		logger.ErrorfX(ctx, "record transaction compensated failed: sagaLog=%v err=%v", sagaLog, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) RecordSagaEnded(ctx context.Context, sagaId int, index int) error {
	now := time.Now().Unix()

	sagaLog := db.SagaLog{
		SagaId:    sagaId,
		Event:     common.SagaEndEvent,
		Index:     index,
		CreatedAt: now,
	}

	if res := db.NewSagaLogDb(ctx, true).DB.Create(&sagaLog); res.Error != nil {
		logger.ErrorfX(ctx, "record transaction compensated failed: sagaLog=%v err=%v", sagaLog, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaLogRepository) GetSagaLogBySagaIds(ctx context.Context, sagaIds []int) (result []db.SagaLog, err error) {
	if result, err = db.NewSagaLogDb(ctx).GetBySagaIds(sagaIds); err != nil {
		logger.ErrorfX(ctx, "get saga by sagaIds failed: sagaIds=%v err=%v", sagaIds, err)
		return result, err
	}

	return result, nil
}
