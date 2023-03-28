package ethereum

import (
	"context"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
)

func (e *EthereumDriver) GetChainTipNumber(ctx context.Context) (uint64, error) {
	blockNum, err := e.client.EthBlockNumber(ctx)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve latest block number: %v", err)
		return 0, err
	}

	return blockNum, nil
}

func (e *EthereumDriver) getBlockByNumber(ctx context.Context, index uint64) (*raw.Block, error) {
	block, err := e.client.EthGetBlockByNumber(index)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve block: %d, %v", index, err)
		return nil, err
	}

	return block, nil
}

func (e *EthereumDriver) getBlockTraceByNumber(ctx context.Context, index uint64) ([]*raw.CallTrace, error) {
	traces, err := e.client.DebugTraceBlock(index)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve block trace: %d, %v", index, err)
		return nil, err
	}

	return traces, nil
}

func (e *EthereumDriver) getBlockReceiptsByNumber(ctx context.Context, index uint64) ([]*raw.TransactionReceipt, error) {
	receipts, err := e.client.GetBlockReceipt(index)
	if err != nil {
		e.logger.Errorf("error thrown while trying to retrieve block receipts: %d, %v", index, err)
		return nil, err
	}

	return receipts, nil
}
