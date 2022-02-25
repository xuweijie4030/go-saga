package repository

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/common"
	"gosaga/app/dao/cache"
	"gosaga/app/dao/db"
	"gosaga/app/util"
	"sync"
	"time"
)

type SagaRepository struct {
}

var (
	sagaRepository     *SagaRepository
	sagaRepositoryOnce sync.Once
)

func NewSagaRepository() *SagaRepository {
	if sagaRepository == nil {
		sagaRepositoryOnce.Do(func() {
			sagaRepository = &SagaRepository{}
		})
	}

	return sagaRepository
}

func (r *SagaRepository) CreateSaga(ctx context.Context, sagaInfo proto.SagaInfo) (id int, err error) {
	content, err := util.JsonMarshalToStr(sagaInfo)
	if err != nil {
		logger.ErrorfX(ctx, "generate content json failed: sagaInfo=%v err=%v", sagaInfo, err)
		return id, err
	}

	now := time.Now().Unix()

	saga := db.Saga{
		TraceId:   common.GetTraceId(ctx),
		Status:    common.SagaRunningStatus,
		Content:   content,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if res := db.NewSagaDb(ctx, true).DB.Create(&saga); res.Error != nil {
		logger.ErrorfX(ctx, "create saga failed: saga=%v err=%v", saga, res.Error)
		return id, res.Error
	}

	id = saga.Id

	return id, nil
}

func (r *SagaRepository) FinishSaga(ctx context.Context, id int) error {
	saga := db.Saga{
		Status:    common.SagaFinishedStatus,
		UpdatedAt: time.Now().Unix(),
	}

	if res := db.NewSagaDb(ctx, true).DB.Model(db.Saga{}).Where("id = ?", id).Select("status", "updated_at").Updates(saga); res.Error != nil {
		logger.ErrorfX(ctx, "finish saga failed: saga=%v err=%v", saga, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaRepository) PendingSaga(ctx context.Context, id int) error {
	saga := db.Saga{
		Status:    common.SagaPendingStatus,
		UpdatedAt: time.Now().Unix(),
	}

	if res := db.NewSagaDb(ctx, true).DB.Model(db.Saga{}).Where("id = ?", id).Select("status", "updated_at").Updates(saga); res.Error != nil {
		logger.ErrorfX(ctx, "pending saga failed: saga=%v err=%v", saga, res.Error)
		return res.Error
	}

	return nil
}

func (r *SagaRepository) GetSagaByIds(ctx context.Context, sagaIds []int) (result []db.Saga, err error) {
	if res := db.NewSagaDb(ctx).DB.Find(&result, sagaIds); res.Error != nil {
		logger.ErrorfX(ctx, "get saga by ids failed: sagaIds=%v err=%v", sagaIds, err)
		return result, err
	}

	return result, nil
}

func (r *SagaRepository) GetWaitRecoverSaga(ctx context.Context) (result []db.Saga, err error) {
	if res := db.NewSagaDb(ctx).DB.Where("status = ?", common.SagaRunningStatus).Find(&result); res.Error != nil {
		logger.ErrorfX(ctx, "get first saga failed: err=%v", err)
		return result, res.Error
	}

	return result, nil
}

func (r *SagaRepository) GetUnfinishedSaga(ctx context.Context) (result []db.Saga, err error) {
	if res := db.NewSagaDb(ctx).DB.Where("status != ?", common.SagaFinishedStatus).Find(&result); res.Error != nil {
		logger.ErrorfX(ctx, "get unfinished saga failed: err=%v", res.Error)
		return result, res.Error
	}

	return result, nil
}

func (r *SagaRepository) AddToContainer(sagaId int) {
	cache.NewContainerCache().Add(sagaId)
}

func (r *SagaRepository) RemoveFromContainer(sagaId int) {
	cache.NewContainerCache().Remove(sagaId)
}

func (r *SagaRepository) GetContainerSagaId() []int {
	return cache.NewContainerCache().Get()
}
