package optimism

import (
	"context"
	"fmt"
	protos "github.com/coherentopensource/chain-interactor/protos/go/protos/chains/optimism"
	"github.com/coherentopensource/go-service-framework/pool"
	"github.com/coherentopensource/go-service-framework/retry"
)

type blockAndReceiptWrapper struct {
	block    *protos.Block
	receipts []*protos.TransactionReceipt
}

// FetchSequence defines the parallelizable steps in the fetch sequence
func (d *OptimismDriver) FetchSequence(blockHeight uint64) map[string]pool.Runner {
	return map[string]pool.Runner{
		stageFetchBlock:  d.queueGetBlockAndTxReceiptByNumber(blockHeight),
		stageFetchTraces: d.queueGetBlockTraceByNumber(blockHeight),
	}
}

// GetChainTipNumber gets the block number of the chaintip
func (d *OptimismDriver) GetChainTipNumber(ctx context.Context) (uint64, error) {
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
func (d *OptimismDriver) getBlockByNumber(ctx context.Context, blockHeight uint64) (*protos.Block, error) {
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

// getBlockAndTransactionReceipt fetches a full block by number, along with the transaction receipt for its first transaction
func (d *OptimismDriver) getTransactionReceipt(ctx context.Context, txHash string) (*protos.TransactionReceipt, error) {
	var txReceipt *protos.TransactionReceipt
	var err error

	if err := retry.Exec(d.config.MaxRetries, func() error {
		txReceipt, err = d.nodeClient.GetTransactionReceipt(ctx, txHash)
		if err != nil {
			d.logger.Warnf("error thrown while trying to retrieve transaction receipt: %d, %v", txHash, err)
			return err
		}
		return nil
	}, nil); err != nil {
		d.logger.Errorf("max retries exceeded trying to get block by number: %v", err)
		return nil, err
	}

	return txReceipt, nil
}

// getBlockTraceByNumber fetches all traces for a given block
func (d *OptimismDriver) getBlockTraceByNumber(ctx context.Context, blockHeight uint64) ([]*protos.CallTrace, error) {
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

// queueGetBlockTraceByNumber wraps GetBlockTraceByNumber in a queueable Runner func
func (d *OptimismDriver) queueGetBlockTraceByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return d.getBlockTraceByNumber(ctx, blockHeight)
	}
}

// queueGetBlockAndTxReceiptByNumber wraps ReadBlockByNumber in a queueable Runner func
func (d *OptimismDriver) queueGetBlockAndTxReceiptByNumber(blockHeight uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, err := d.getBlockByNumber(ctx, blockHeight)
		if err != nil {
			return nil, err
		}

		if len(block.Transactions) == 0 {
			return nil, fmt.Errorf("no transactions present in block %d", blockHeight)
		}

		receipts := make([]*protos.TransactionReceipt, 0)
		for _, tx := range block.Transactions {
			txReceipt, err := d.getTransactionReceipt(ctx, tx.Hash)
			if err != nil {
				d.logger.Errorf("error fetching transaction receipt with hash: %s, %v", tx.Hash, err)
			}
			receipts = append(receipts, txReceipt)
		}

		return &blockAndReceiptWrapper{block: block, receipts: receipts}, nil
	}
}
