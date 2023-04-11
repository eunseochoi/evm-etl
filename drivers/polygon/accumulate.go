package polygon

import (
	"context"
	"errors"
	protos "github.com/datadaodevs/evm-etl/protos/go/protos/chains/polygon"
	"github.com/datadaodevs/go-service-framework/pool"
)

// Accumulate combines a block, receipts, and traces from multiple protos into a single object, given a generic
// result from the "fetch" step
func (p *Driver) Accumulate(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		set, ok := res.(pool.ResultSet)
		if !ok {
			return nil, errors.New("result is not expected type")
		}

		block, err := extractBlock(set)
		if err != nil {
			return nil, err
		}
		receipts, err := extractReceipts(set)
		if err != nil {
			return nil, err
		}
		traces, err := extractTraces(set)
		if err != nil {
			return nil, err
		}

		return &protos.Data{
			Block:               block,
			TransactionReceipts: receipts,
			CallTraces:          traces,
		}, nil
	}
}

// extractBlock extracts a block from the generic ResultSet from the fetch step
func extractBlock(set pool.ResultSet) (*protos.Block, error) {
	blockRes, ok := set[stageFetchBlock]
	if !ok {
		return nil, errors.New("No block data")
	}
	block, ok := blockRes.(*protos.Block)
	if !ok {
		return nil, errors.New("incorrect data type for block")
	}

	return block, nil
}

// extractReceipts extracts receipts from the generic ResultSet from the fetch step
func extractReceipts(set pool.ResultSet) ([]*protos.TransactionReceipt, error) {
	receiptsRes, ok := set[stageFetchReceipt]
	if !ok {
		return nil, errors.New("No receipts data")
	}
	receipts, ok := receiptsRes.([]*protos.TransactionReceipt)
	if !ok {
		return nil, errors.New("incorrect data type for transaction receipts")
	}

	return receipts, nil
}

// extractTraces extracts traces from the generic ResultSet from the fetch step
func extractTraces(set pool.ResultSet) ([]*protos.CallTrace, error) {
	tracesRes, ok := set[stageFetchTraces]
	if !ok {
		return nil, errors.New("No traces data")
	}
	traces, ok := tracesRes.([]*protos.CallTrace)
	if !ok {
		return nil, errors.New("incorrect data type for traces")
	}

	return traces, nil
}
