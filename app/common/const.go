package common

// ctx filed key
const (
	SagaTraceIdKey = "saga_trace_id"
	SagaSpanIdKey  = "saga_span_id"
)

// saga status
const (
	SagaRunningStatus    = 0
	SagaFinishedStatus   = 1
	SagaPendingStatus    = 2
	SagaRecoveringStatus = 3
)

const GetWaitRecoverSagaLimit = 100

// saga log event
const (
	SagaCreatedEvent = iota
	TransactionBeginEvent
	TransactionEndEvent
	TransactionAbortedEvent
	TransactionCompensateEvent
	SagaEndEvent
)

const (
	RecoverTransactionHandleType = 1
	RecoverCompensateHandleType  = 2
	RecordSagaLogEndHandleType   = 3
	RecordSagaEndHandleType      = 4
)

const (
	ContainerAddHandleType    = 1
	ContainerRemoveHandleType = 2
)

const (
	SingleRecoverType  = "single"
	ClusterRecoverType = "cluster"
)
