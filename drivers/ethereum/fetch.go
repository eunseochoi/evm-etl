package ethereum

import (
	"context"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/datadaodevs/go-service-framework/pool"
)

// FetchSequence defines the parallelizable steps in the fetch sequence
func (e *EthereumDriver) FetchSequence(index uint64) map[string]pool.Runner {
	return map[string]pool.Runner{
		stageFetchBlock:   e.queueGetBlockByNumber(index),
		stageFetchReceipt: e.queueGetBlockReceiptsByNumber(index),
		stageFetchTraces:  e.queueGetBlockTraceByNumber(index),
	}
}

// GetChainTipNumber gets the block number of the chaintip
func (e *EthereumDriver) GetChainTipNumber(ctx context.Context) (uint64, error) {
	blockNum, err := e.client.EthBlockNumber(ctx)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve latest block number: %v", err)
		return 0, err
	}

	return blockNum, nil
}

// getBlockByNumber fetches a full block by number
func (e *EthereumDriver) getBlockByNumber(ctx context.Context, index uint64) (*raw.Block, error) {
	block, err := e.client.EthGetBlockByNumber(ctx, index)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve block: %d, %v", index, err)
		return nil, err
	}

	return block, nil
}

// getBlockTraceByNumber fetches all traces for a given block
func (e *EthereumDriver) getBlockTraceByNumber(ctx context.Context, index uint64) ([]*raw.CallTrace, error) {
	traces, err := e.client.DebugTraceBlock(ctx, index)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve block trace: %d, %v", index, err)
		return nil, err
	}

	return traces, nil
}

// getBlockReceiptsByNumber fetches a set of block receipts for a given block
func (e *EthereumDriver) getBlockReceiptsByNumber(ctx context.Context, index uint64) ([]*raw.TransactionReceipt, error) {
	receipts, err := e.client.GetBlockReceipt(ctx, index)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve block receipts: %d, %v", index, err)
		return nil, err
	}

	return receipts, nil
}

// queueGetBlockTraceByNumber wraps GetBlockTraceByNumber in a queueable Runner func
func (e *EthereumDriver) queueGetBlockTraceByNumber(index uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockTraceByNumber(ctx, index)
	}
}

// queueGetBlockByNumber wraps GetBlockByNumber in a queueable Runner func
func (e *EthereumDriver) queueGetBlockByNumber(index uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockByNumber(ctx, index)
	}
}

// queueGetBlockReceiptsByNumber wraps GetBlockReceiptsByNumber in a queueable Runner func
func (e *EthereumDriver) queueGetBlockReceiptsByNumber(index uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockReceiptsByNumber(ctx, index)
	}
}
