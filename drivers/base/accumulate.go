package base

import (
	"context"
	"errors"
	protos "github.com/coherentopensource/evm-etl/protos/go/protos/chains/base"
	"github.com/coherentopensource/go-service-framework/pool"
)

// Accumulate combines a block, receipts, and traces from multiple protos into a single object, given a generic
// result from the "fetch" step
func (d *Driver) Accumulate(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		set, ok := res.(pool.ResultSet)
		if !ok {
			return nil, errors.New("result is not expected type")
		}

		block, receipts, err := extractBlockAndReceipts(set)
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
func extractBlockAndReceipts(set pool.ResultSet) (*protos.Block, []*protos.TransactionReceipt, error) {
	blockRes, ok := set[stageFetchBlock]
	if !ok {
		return nil, nil, errors.New("No block data")
	}
	wrapper, ok := blockRes.(*blockAndReceiptWrapper)
	if !ok {
		return nil, nil, errors.New("incorrect data type for block/receipt wrapper")
	}

	return wrapper.block, []*protos.TransactionReceipt{wrapper.receipt}, nil
}

// extractTraces extracts traces from the generic ResultSet from the fetch step
func extractTraces(set pool.ResultSet) ([]*protos.CallTrace, error) {
	tracesRes, ok := set[stageFetchTraces]
	if !ok {
		return nil, errors.New("no traces data")
	}
	traces, ok := tracesRes.([]*protos.CallTrace)
	if !ok {
		return nil, errors.New("incorrect data type for traces")
	}

	return traces, nil
}
