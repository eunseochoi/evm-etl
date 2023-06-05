package arbitrum

import (
	"context"
	"errors"
)

// IsValidBlock checks the given block's parent hash against the hash of the previous block
func (d *Driver) IsValidBlock(ctx context.Context, index uint64) error {
	d.logger.Infof("Comparing block %d to block %d for validation", index, index-1)

	currentBlock, err := d.getBlockByNumber(ctx, index)
	if err != nil {
		return err
	}
	previousBlock, err := d.store.RetrieveBlock(ctx, index-1)
	if err != nil {
		return err
	}

	if currentBlock.ParentHash != previousBlock.Hash {
		d.logger.Infof("chain reorg detected at block %d", previousBlock.Number)
		return errors.New("New block parent hash does not match previous block hash")
	}

	return nil
}
