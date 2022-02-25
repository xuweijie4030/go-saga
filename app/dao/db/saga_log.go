package db

import (
	"context"
	"github.com/carefreex-io/dbdao/gormdb"
	"gorm.io/gorm"
)

type SagaLog struct {
	Id        int    `gorm:"primaryKey" json:"id"`
	SagaId    int    `json:"saga_id"`
	SpanId    string `json:"span_id"`
	Event     int    `json:"event"`
	Index     int    `json:"index"`
	Content   string `json:"content"`
	CreatedAt int64  `json:"created_at"`
}

type SagaLogDb struct {
	DB *gorm.DB
}

func NewSagaLogDb(ctx context.Context, arg ...bool) (db *SagaLogDb) {
	db = &SagaLogDb{
		DB: gormdb.Read,
	}
	if len(arg) != 0 && arg[0] {
		db.DB = gormdb.Write
	}
	db.DB.WithContext(ctx)

	return db
}

func (d *SagaLogDb) TableName() string {
	return "saga_log"
}

