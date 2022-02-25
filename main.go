package main

import (
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/dbdao/gormdb"
	"github.com/carefreex-io/logger"
	"github.com/carefreex-io/rpcxserver"
	"gosaga/app/common"
	"gosaga/app/daemon"
)

func main() {
	config.InitConfig()

	logger.DefaultCustomOptions.CtxFiles = []string{common.SagaTraceIdKey, common.SagaSpanIdKey}
	logger.InitLogger()

	if err := gormdb.InitDB(); err != nil {
		logger.Fatalf("mysql.InitDB failed: err=%v", err)
	}

	rpcxserver.DefaultCustomOptions.Service = NewService()
	s := rpcxserver.NewServer()

	daemon.Register(s)

	s.Start()
}
