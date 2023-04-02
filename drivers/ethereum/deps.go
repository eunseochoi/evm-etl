package ethereum

import (
	"context"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
)

type Store interface {
	WriteOne(ctx context.Context, input interface{}, mapToStruct interface{}, filename string) error
	WriteMany(ctx context.Context, input []interface{}, mapToStruct interface{}, filename string) error
	RetrieveBlock(ctx context.Context, blockHeight uint64) (*raw.Block, error)
}
