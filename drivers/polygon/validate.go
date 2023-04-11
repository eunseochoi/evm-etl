package polygon

import (
	"context"
	"errors"
)

// IsValidBlock checks the given block's parent hash against the hash of the previous block
func (p *Driver) IsValidBlock(ctx context.Context, index uint64) error {
	p.logger.Infof("comparing block %d to block %d for validation", index, index-1)

	currentBlock, err := p.getBlockByNumber(ctx, index)
	if err != nil {
		return err
	}
	previousBlock, err := p.store.RetrieveBlock(ctx, index-1)
	if err != nil {
		return err
	}

	if currentBlock.ParentHash != previousBlock.Hash {
		p.logger.Infof("chain reorg detected at block %d", previousBlock.Number)
		return errors.New("new block parent hash does not match previous block hash")
	}

	return nil
}
