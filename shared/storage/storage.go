package storage

import (
	"context"
)

type Store interface {
	WriteOne(ctx context.Context, input interface{}, mapToStruct interface{}, filename string) error
	WriteMany(ctx context.Context, input []interface{}, mapToStruct interface{}, filename string) error
	ProjectID() string
	Bucket() string
	RangeSize() uint64
}
