package commonlogic

import (
	"gosaga/app/dao/db"
	"sync"
)

type SagaLogCommonLogic struct {
}

var (
	sagaLogCommonLogic     *SagaLogCommonLogic
	sagaLogCommonLogicOnce sync.Once
)

func NewSagaLogCommonLogic() *SagaLogCommonLogic {
	if sagaLogCommonLogic == nil {
		sagaLogCommonLogicOnce.Do(func() {
			sagaLogCommonLogic = &SagaLogCommonLogic{}
		})
	}

	return sagaLogCommonLogic
}

func (l SagaLogCommonLogic) GetSagaLogMap(sagaLogs []db.SagaLog) (result map[int][]db.SagaLog) {
	result = make(map[int][]db.SagaLog)

	if len(sagaLogs) == 0 {
		return result
	}

	for _, sagaLog := range sagaLogs {
		sagaLogSli, ok := result[sagaLog.SagaId]
		if !ok {
			sagaLogSli = make([]db.SagaLog, 0)
		}
		sagaLogSli = append(sagaLogSli, sagaLog)
		result[sagaLog.SagaId] = sagaLogSli
	}

	return result
}
