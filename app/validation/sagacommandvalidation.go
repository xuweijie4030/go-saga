package validation

import (
	"errors"
	"fmt"
	"github.com/xuweijie4030/go-common/gosaga"
	"github.com/xuweijie4030/go-common/gosaga/proto"
)

func SagaCommandService_SubmitSagaValidate(request *proto.SubmitSagaRequest) (err error) {
	if len(request.Transaction) < 1 {
		err = errors.New("at least one transaction")

		return err
	}

	if request.ExecType != gosaga.SerialExecType && request.ExecType != gosaga.ConcurrentExecType {
		err = errors.New("not supported saga ExecType")
		return err
	}

	if request.GlobalCompensateType > gosaga.ForwardCompensateType || request.GlobalCompensateType < 0 {
		err = errors.New("not supported saga GlobalCompensateType")
		return err
	}

	if request.ExecType == gosaga.ConcurrentExecType && request.GlobalCompensateType == 0 {
		err = errors.New("when saga ExecType is concurrent, only use GlobalCompensateType define compensate type")
		return err
	}

	isHaveForwardCompensateType := false
	for i, transaction := range request.Transaction {
		if transaction.CallType != gosaga.RpcTransaction && transaction.CallType != gosaga.ApiTransaction {
			err = errors.New(fmt.Sprintf("not supported transaction CallType: transactionIndex=%v", i))
			return err
		}
		if transaction.CallType == gosaga.RpcTransaction && (transaction.Action == "" || transaction.ServerName == "") {
			err = errors.New(fmt.Sprintf("when CallType eq 1, Action and ServerName is required : transactionIndex=%v", i))
			return err
		}
		if transaction.CompensateType > gosaga.ForwardCompensateType || transaction.CompensateType < 0 {
			err = errors.New("not supported transaction CompensateType")
			return err
		}
		if transaction.CompensateType == 0 && request.GlobalCompensateType == 0 {
			err = errors.New(fmt.Sprintf("when saga GlobalCompensateType eq 0, transaction CompensateType is requeired: transactionIndex=%v", i))
			return err
		}
		if request.GlobalCompensateType != 0 {
			continue
		}
		if isHaveForwardCompensateType && transaction.CompensateType == gosaga.BackwardCompensateType {
			err = errors.New(fmt.Sprintf("before and backward compensate transaction are not allowed behind forward compensate transaction: transactionIndex=%v", i))
			return err
		}
		if transaction.CompensateType == gosaga.ForwardCompensateType {
			isHaveForwardCompensateType = true
		}
	}

	return nil
}

func SagaCommandService_RecoverSagaValidate(request *proto.RecoverSagaRequest) (err error) {
	if len(request.SagaId) < 1 {
		err = errors.New("at least one saga id")

		return err
	}

	for i, id := range request.SagaId {
		if id == 0 {
			err = errors.New(fmt.Sprintf("saga id not eq 0: index=%v", i))
			return err
		}
	}

	return nil
}
