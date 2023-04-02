package ethereum

import (
	"context"
	model "github.com/datadaodevs/evm-etl/model/ethereum"
)

type Store interface {
	WriteOne(ctx context.Context, input interface{}, mapToStruct interface{}, filename string) error
	WriteMany(ctx context.Context, input []interface{}, mapToStruct interface{}, filename string) error
	RetrieveBlock(ctx context.Context, blockHeight uint64) (*model.ParquetBlock, error)
}
