package ethereum

import (
	"context"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/datadaodevs/go-service-framework/pool"
	"github.com/datadaodevs/go-service-framework/retry"
)

// FetchSequence defines the parallelizable steps in the fetch sequence
func (e *EthereumDriver) FetchSequence(blockHeight uint64) map[string]pool.Runner {
	return map[string]pool.Runner{
		stageFetchBlock:   e.queueGetBlockByNumber(blockHeight),
		stageFetchReceipt: e.queueGetBlockReceiptsByNumber(blockHeight),
		stageFetchTraces:  e.queueGetBlockTraceByNumber(blockHeight),
	}
}

// GetChainTipNumber gets the block number of the chaintip
func (e *EthereumDriver) GetChainTipNumber(ctx context.Context) (uint64, error) {
	var blockNum uint64
	var err error
	if err := retry.Exec(e.config.MaxRetries, func() error {
		blockNum, err = e.nodeClient.EthBlockNumber(ctx)
		if err != nil {
			e.logger.Warnf("error thrown while trying to retrieve latest block number: %v", err)
			return err
		}
		return nil
	}, nil); err != nil {
		e.logger.Errorf("Max retries exceeded trying to get chaintip number: %v", err)
		return 0, err
	}

	return blockNum, nil
}

// getBlockByNumber fetches a full block by number
func (e *EthereumDriver) getBlockByNumber(ctx context.Context, blockHeight uint64) (*raw.Block, error) {
	var block *raw.Block
	var err error
	if err := retry.Exec(e.config.MaxRetries, func() error {
		block, err = e.nodeClient.EthGetBlockByNumber(ctx, blockHeight)
		if err != nil {
			e.logger.Warnf("error thrown while trying to retrieve block: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		e.logger.Errorf("Max retries exceeded trying to get block by number: %v", err)
		return nil, err
	}

	return block, nil
}

// getBlockTraceByNumber fetches all traces for a given block
func (e *EthereumDriver) getBlockTraceByNumber(ctx context.Context, blockHeight uint64) ([]*raw.CallTrace, error) {
	var traces []*raw.CallTrace
	var err error
	if err := retry.Exec(e.config.MaxRetries, func() error {
		traces, err = e.nodeClient.DebugTraceBlock(ctx, blockHeight)
		if err != nil {
			e.logger.Warnf("error thrown while trying to retrieve block trace: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		e.logger.Errorf("Max retries exceeded trying to get traces: %v", err)
		return nil, err
	}

	return traces, nil
}

// getBlockReceiptsByNumber fetches a set of block receipts for a given block
func (e *EthereumDriver) getBlockReceiptsByNumber(ctx context.Context, blockHeight uint64) ([]*raw.TransactionReceipt, error) {
	var receipts []*raw.TransactionReceipt
	var err error
	if err := retry.Exec(e.config.MaxRetries, func() error {
		receipts, err = e.nodeClient.GetBlockReceipt(ctx, blockHeight)
		if err != nil {
			e.logger.Warnf("error thrown while trying to retrieve block receipts: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		e.logger.Errorf("Max retries exceeded trying to get receipts: %v", err)
		return nil, err
	}

	return receipts, nil
}

// queueGetBlockTraceByNumber wraps GetBlockTraceByNumber in a queueable Runner func
func (e *EthereumDriver) queueGetBlockTraceByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockTraceByNumber(ctx, blockHeight)
	}
}

// queueGetBlockByNumber wraps GetBlockByNumber in a queueable Runner func
func (e *EthereumDriver) queueGetBlockByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockByNumber(ctx, blockHeight)
	}
}

// queueGetBlockReceiptsByNumber wraps GetBlockReceiptsByNumber in a queueable Runner func
func (e *EthereumDriver) queueGetBlockReceiptsByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockReceiptsByNumber(ctx, blockHeight)
	}
}
