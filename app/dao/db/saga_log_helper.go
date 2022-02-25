package db

import "gosaga/app/common"

func (d *SagaLogDb) GetBySagaIds(sagaIds []int) (result []SagaLog, err error) {
	res := d.DB.Where("saga_id IN ?", sagaIds).Order("saga_id asc, `index` asc, created_at asc").Find(&result)
	err = res.Error

	return result, err
}

func (d *SagaLogDb) GetBySpanIds(spanIds []string) (result []SagaLog, err error) {
	res := d.DB.Where("span_id IN ? AND event = ?", spanIds, common.TransactionBeginEvent).Find(&result)
	err = res.Error

	return result, err
}
