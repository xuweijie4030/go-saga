package main

import (
	"context"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"gosaga/app/service"
	"gosaga/app/validation"
)

type Service struct {
	SagaCommandService *service.SagaCommandService
	SagaQueryService *service.SagaQueryService
}

func NewService() *Service {
	return &Service{
		SagaCommandService: service.NewSagaCommandService(),
		SagaQueryService: service.NewSagaQueryService(),
	}
}

func (s *Service) SubmitSaga (ctx context.Context, request *proto.SubmitSagaRequest, response *proto.SubmitSagaResponse) (err error) {
	if err = validation.SagaCommandService_SubmitSagaValidate(request); err != nil {
		return err
	}

	return s.SagaCommandService.SubmitSaga(ctx, request, response)
}

func (s *Service) RecoverSaga (ctx context.Context, request *proto.RecoverSagaRequest, response *proto.RecoverSagaResponse) (err error) {
	if err = validation.SagaCommandService_RecoverSagaValidate(request); err != nil {
		return err
	}

	return s.SagaCommandService.RecoverSaga(ctx, request, response)
}

func (s *Service) GetRunningSagaId (ctx context.Context, request *proto.GetRunningSagaIdRequest, response *proto.GetRunningSagaIdResponse) (err error) {
	return s.SagaQueryService.GetRunningSagaId(ctx, request, response)
}


