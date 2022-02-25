package logic

import (
	"context"
	"gosaga/app/repository"
	"sync"
)

type SagaQueryLogic struct {
}

var (
	sagaQueryLogic     *SagaQueryLogic
	sagaQueryLogicOnce sync.Once
)

func NewSagaQueryLogic() *SagaQueryLogic {
	if sagaQueryLogic != nil {
		return sagaQueryLogic
	}
	sagaQueryLogicOnce.Do(func() {
		sagaQueryLogic = &SagaQueryLogic{}
	})

	return sagaQueryLogic
}

func (l *SagaQueryLogic) GetRunningSagaId(ctx context.Context) []int {
	return repository.NewSagaRepository().GetContainerSagaId()
}
