package db

import (
	"context"
	"github.com/carefreex-io/dbdao/gormdb"
	"gorm.io/gorm"
)

type Saga struct {
	Id        int    `gorm:"primaryKey" json:"id"`
	TraceId   string `json:"trace_id"`
	Content   string `json:"content"`
	Status    int    `json:"status"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type SagaDb struct {
	DB *gorm.DB
}

func NewSagaDb(ctx context.Context, arg ...bool) (db *SagaDb) {
	db = &SagaDb{
		DB: gormdb.Read,
	}
	if len(arg) != 0 && arg[0] {
		db.DB = gormdb.Write
	}
	db.DB.WithContext(ctx)

	return db
}

func (d *SagaDb) TableName() string {
	return "saga"
}

