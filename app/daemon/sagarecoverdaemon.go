package daemon

import (
	"context"
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosagarecover/proto"
	"github.com/xuweijie4030/go-common/gosagarecover/rpc"
	"gosaga/app/common"
	"gosaga/app/logic"
	"gosaga/app/logic/commonlogic"
)

type SagaRecoverDaemon struct {
}

func NewSagaRecoverDaemon() *SagaRecoverDaemon {
	return &SagaRecoverDaemon{}
}

func (d *SagaRecoverDaemon) Handler() {
	ctx := context.Background()

	if config.GetString("SagaRecover.Type") == common.ClusterRecoverType {
		if err := d.sendRecoverSignal(ctx); err != nil {
			logger.ErrorfX(ctx, "d.sendRecoverSignal failed: err=%v", err)
			return
		}
		return
	}

	sagas, sagaLogs, err := logic.NewSagaRecoverLogic().GetWaitRecoverSagaInfo(ctx)
	if err != nil {
		logger.ErrorfX(ctx, "get wait recover saga failed: err=%v", err)
		return
	}
	if len(sagas) == 0 {
		logger.InfofX(ctx, "wait recover saga finished processing")
		return
	}
	sagaLogMap := commonlogic.NewSagaLogCommonLogic().GetSagaLogMap(sagaLogs)
	for _, saga := range sagas {
		ctx = context.WithValue(ctx, common.SagaTraceIdKey, saga.TraceId)
		go commonlogic.NewSagaRecoverCommonLogic().Recover(ctx, saga, sagaLogMap[saga.Id])
	}
}

func (d *SagaRecoverDaemon) sendRecoverSignal(ctx context.Context) (err error) {
	cli, err := rpc.NewClient()
	if err != nil {
		logger.ErrorfX(ctx, "new saga recover client failed: err=%v", err)
		return err
	}

	request := proto.RecoverRequest{}
	response := proto.RecoverResponse{}
	if err = cli.Recover(ctx, &request, &response); err != nil {
		logger.ErrorfX(ctx, "GoSagaRecover.Recover failed: err=%v", err)
		return err
	}

	return nil
}
