package service

import (
	"context"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/logic"
)

type SagaQueryService struct {
}

func NewSagaQueryService() *SagaQueryService {
	return &SagaQueryService{}
}

func (s *SagaQueryService) GetRunningSagaId(ctx context.Context, request *proto.GetRunningSagaIdRequest, response *proto.GetRunningSagaIdResponse) error {
	response.SagaId = logic.NewSagaQueryLogic().GetRunningSagaId(ctx)

	logger.InfofX(ctx, "running saga id=%v", response.SagaId)

	return nil
}
