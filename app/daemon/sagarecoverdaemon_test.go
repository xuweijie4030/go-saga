package daemon

import (
	"fmt"
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/dbdao/gormdb"
	"github.com/carefreex-io/logger"
	"gosaga/app/common"
	"testing"
)

func TestSagaRecoverDaemon_Handler(t *testing.T) {
	config.DefaultCustomOptions.Path = "../../conf/conf.yaml"
	config.InitConfig()

	logger.DefaultCustomOptions.CtxFiles = []string{common.SagaTraceIdKey, common.SagaSpanIdKey}
	logger.InitLogger()

	if err := gormdb.InitDB(); err != nil {
		fmt.Printf("init db failed: err=%v", err)
		return
	}

	NewSagaRecoverDaemon().Handler()

	select {

	}
}
