package polygon

import (
	"context"
	protos "github.com/coherentopensource/chain-interactor/protos/go/protos/chains/polygon"
	"github.com/coherentopensource/go-service-framework/pool"
	"github.com/coherentopensource/go-service-framework/retry"
)

// FetchSequence defines the parallelizable steps in the fetch sequence
func (p *Driver) FetchSequence(blockHeight uint64) map[string]pool.Runner {
	return map[string]pool.Runner{
		stageFetchBlock:   p.queueGetBlockByNumber(blockHeight),
		stageFetchReceipt: p.queueGetBlockReceiptsByNumber(blockHeight),
		stageFetchTraces:  p.queueGetBlockTraceByNumber(blockHeight),
	}
}

// GetChainTipNumber gets the block number of the chaintip
func (p *Driver) GetChainTipNumber(ctx context.Context) (uint64, error) {
	var blockNum uint64
	var err error
	if err := retry.Exec(p.config.MaxRetries, func() error {
		blockNum, err = p.nodeClient.GetLatestBlockNumber(ctx)
		if err != nil {
			p.logger.Warnf("error thrown while trying to retrieve latest block number: %v", err)
			return err
		}
		return nil
	}, nil); err != nil {
		p.logger.Errorf("max retries exceeded trying to get chaintip number: %v", err)
		return 0, err
	}

	return blockNum, nil
}

// getBlockByNumber fetches a full block by number
func (p *Driver) getBlockByNumber(ctx context.Context, blockHeight uint64) (*protos.Block, error) {
	var block *protos.Block
	var err error
	if err := retry.Exec(p.config.MaxRetries, func() error {
		block, err = p.nodeClient.GetBlockByNumber(ctx, blockHeight)
		if err != nil {
			p.logger.Warnf("error thrown while trying to retrieve block: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		p.logger.Errorf("max retries exceeded trying to get block by number: %v", err)
		return nil, err
	}

	return block, nil
}

// getBlockTraceByNumber fetches all traces for a given block
func (p *Driver) getBlockTraceByNumber(ctx context.Context, blockHeight uint64) ([]*protos.CallTrace, error) {
	var traces []*protos.CallTrace
	var err error
	if err := retry.Exec(p.config.MaxRetries, func() error {
		traces, err = p.nodeClient.GetTracesForBlock(ctx, blockHeight)
		if err != nil {
			p.logger.Warnf("error thrown while trying to retrieve block trace: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		p.logger.Errorf("max retries exceeded trying to get traces: %v", err)
		return nil, err
	}

	return traces, nil
}

// getBlockReceiptsByNumber fetches a set of block receipts for a given block
func (p *Driver) getBlockReceiptsByNumber(ctx context.Context, blockHeight uint64) ([]*protos.TransactionReceipt, error) {
	var receipts []*protos.TransactionReceipt
	var err error
	if err := retry.Exec(p.config.MaxRetries, func() error {
		receipts, err = p.nodeClient.GetBlockReceipt(ctx, blockHeight)
		if err != nil {
			p.logger.Warnf("error thrown while trying to retrieve block receipts: %d, %v", blockHeight, err)
			return err
		}

		return nil
	}, nil); err != nil {
		p.logger.Errorf("max retries exceeded trying to get receipts: %v", err)
		return nil, err
	}

	return receipts, nil
}

// queueGetBlockTraceByNumber wraps GetBlockTraceByNumber in a queueable Runner func
func (p *Driver) queueGetBlockTraceByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return p.getBlockTraceByNumber(ctx, blockHeight)
	}
}

// queueGetBlockByNumber wraps getBlockByNumber in a queueable Runner func
func (p *Driver) queueGetBlockByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return p.getBlockByNumber(ctx, blockHeight)
	}
}

// queueGetBlockReceiptsByNumber wraps getBlockReceiptsByNumber in a queueable Runner func
func (p *Driver) queueGetBlockReceiptsByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return p.getBlockReceiptsByNumber(ctx, blockHeight)
	}
}
