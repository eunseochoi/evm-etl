package ethereum

import (
	"context"
	"errors"
)

// IsValidBlock checks the given block's parent hash against the hash of the previous block
func (e *EthereumDriver) IsValidBlock(ctx context.Context, index uint64) error {
	currentBlock, err := e.getBlockByNumber(ctx, index)
	if err != nil {
		return err
	}
	previousBlock, err := e.store.RetrieveBlock(ctx, index-1)
	if err != nil {
		return err
	}

	if currentBlock.ParentHash != previousBlock.Hash {
		e.logger.Infof("chain reorg detected at block %d", previousBlock.Number)
		return errors.New("New block parent hash does not match previous block hash")
	}

	return nil
}
