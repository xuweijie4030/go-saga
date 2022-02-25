package common

import (
	"context"
	"github.com/spf13/cast"
	"github.com/xuweijie4030/go-common/gosaga/proto"
)

func GetTraceId(ctx context.Context) string {
	return cast.ToString(ctx.Value(SagaTraceIdKey))
}

func GetSpanId(ctx context.Context) string {
	return cast.ToString(ctx.Value(SagaSpanIdKey))
}

func GetSagaContent(content string) (sagaInfo proto.SubmitSagaRequest, err error) {
	return
}
