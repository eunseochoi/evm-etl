package binance

import (
	"context"
	protos "github.com/coherentopensource/evm-etl/protos/go/protos/chains/binance"
	"github.com/coherentopensource/go-service-framework/pool"
	"github.com/coherentopensource/go-service-framework/retry"
)

// FetchSequence defines the parallelizable steps in the fetch sequence
func (d *Driver) FetchSequence(blockHeight uint64) map[string]pool.Runner {
	return map[string]pool.Runner{
		stageFetchBlock:   d.queueGetBlockByNumber(blockHeight),
		stageFetchReceipt: d.queueGetBlockReceiptsByNumber(blockHeight),
		stageFetchTraces:  d.queueGetBlockTraceByNumber(blockHeight),
	}
}

// GetChainTipNumber gets the block number of the chaintip
func (d *Driver) GetChainTipNumber(ctx context.Context) (uint64, error) {
	var blockNum uint64
	var err error
	if err := retry.Exec(d.config.MaxRetries, func() error {
		blockNum, err = d.nodeClient.GetLatestBlockNumber(ctx)
		if err != nil {
			d.logger.Warnf("error thrown while trying to retrieve latest block number: %v", err)
			return err
		}
		return nil
	}, nil); err != nil {
		d.logger.Errorf("max retries exceeded trying to get chaintip number: %v", err)
		return 0, err
	}

	return blockNum, nil
}

// getBlockByNumber fetches a full block by number
func (d *Driver) getBlockByNumber(ctx context.Context, blockHeight uint64) (*protos.Block, error) {
	var block *protos.Block
	var err error
	if err := retry.Exec(d.config.MaxRetries, func() error {
		block, err = d.nodeClient.GetBlockByNumber(ctx, blockHeight)
		if err != nil {
			d.logger.Warnf("error thrown while trying to retrieve block: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		d.logger.Errorf("max retries exceeded trying to get block by number: %v", err)
		return nil, err
	}

	return block, nil
}

// getBlockTraceByNumber fetches all traces for a given block
func (d *Driver) getBlockTraceByNumber(ctx context.Context, blockHeight uint64) ([]*protos.CallTrace, error) {
	var traces []*protos.CallTrace
	var err error
	if err := retry.Exec(d.config.MaxRetries, func() error {
		traces, err = d.nodeClient.GetTracesForBlock(ctx, blockHeight)
		if err != nil {
			d.logger.Warnf("error thrown while trying to retrieve block trace: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		d.logger.Errorf("max retries exceeded trying to get traces: %v", err)
		return nil, err
	}

	return traces, nil
}

// getBlockReceiptsByNumber fetches a set of block receipts for a given block
func (d *Driver) getBlockReceiptsByNumber(ctx context.Context, blockHeight uint64) ([]*protos.TransactionReceipt, error) {
	var receipts []*protos.TransactionReceipt
	var err error
	if err := retry.Exec(d.config.MaxRetries, func() error {
		receipts, err = d.nodeClient.GetBlockReceipt(ctx, blockHeight)
		if err != nil {
			d.logger.Warnf("error thrown while trying to retrieve block receipts: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		d.logger.Errorf("max retries exceeded trying to get receipts: %v", err)
		return nil, err
	}

	return receipts, nil
}

// queueGetBlockTraceByNumber wraps GetBlockTraceByNumber in a queueable Runner func
func (d *Driver) queueGetBlockTraceByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return d.getBlockTraceByNumber(ctx, blockHeight)
	}
}

// queueGetBlockByNumber wraps getBlockByNumber in a queueable Runner func
func (d *Driver) queueGetBlockByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return d.getBlockByNumber(ctx, blockHeight)
	}
}

// queueGetBlockReceiptsByNumber wraps getBlockReceiptsByNumber in a queueable Runner func
func (d *Driver) queueGetBlockReceiptsByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return d.getBlockReceiptsByNumber(ctx, blockHeight)
	}
}
