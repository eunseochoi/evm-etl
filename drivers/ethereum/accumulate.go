package ethereum

import (
	"context"
	"errors"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/datadaodevs/go-service-framework/pool"
)

// Accumulate combines a block, receipts, and traces from multiple protos into a single object, given a generic
// result from the "fetch" step
func (e *EthereumDriver) Accumulate(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		set, ok := res.(pool.ResultSet)
		if !ok {
			return nil, errors.New("Result is not expected type")
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

		return &raw.Data{
			Block:               block,
			TransactionReceipts: receipts,
			CallTraces:          traces,
		}, nil
	}
}

// extractBlock extracts a block from the generic ResultSet from the fetch step
func extractBlock(set pool.ResultSet) (*raw.Block, error) {
	blockRes, ok := set[stageFetchBlock]
	if !ok {
		return nil, errors.New("No block data")
	}
	block, ok := blockRes.(*raw.Block)
	if !ok {
		return nil, errors.New("Incorrect data type for block")
	}

	return block, nil
}

// extractReceipts extracts receipts from the generic ResultSet from the fetch step
func extractReceipts(set pool.ResultSet) ([]*raw.TransactionReceipt, error) {
	receiptsRes, ok := set[stageFetchReceipt]
	if !ok {
		return nil, errors.New("No receipts data")
	}
	receipts, ok := receiptsRes.([]*raw.TransactionReceipt)
	if !ok {
		return nil, errors.New("Incorrect data type for transaction receipts")
	}

	return receipts, nil
}

// extractTraces extracts traces from the generic ResultSet from the fetch step
func extractTraces(set pool.ResultSet) ([]*raw.CallTrace, error) {
	tracesRes, ok := set[stageFetchTraces]
	if !ok {
		return nil, errors.New("No traces data")
	}
	traces, ok := tracesRes.([]*raw.CallTrace)
	if !ok {
		return nil, errors.New("Incorrect data type for traces")
	}

	return traces, nil
}
