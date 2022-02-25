package rpc

import (
	"context"
	"fmt"
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/logger"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"testing"
)

func TestClient_SubmitSaga(t *testing.T) {
	config.DefaultCustomOptions.Path = "../conf/conf.yaml"
	config.InitConfig()

	logger.InitLogger()

	ctx := context.Background()
	request := proto.SubmitSagaRequest{

	}
	response := proto.SubmitSagaResponse{}

	cli, err := NewClient()
	if err != nil {
		fmt.Printf("new cline failed: err=%v\n", err)
	}
	if err = cli.SubmitSaga(ctx, &request, &response); err != nil {
		fmt.Printf("call hello failed: err=%v\n", err)
	}
	fmt.Println(response)
}
