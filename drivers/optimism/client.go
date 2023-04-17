package optimism

import (
	"context"
	"fmt"
	"github.com/coherentopensource/chain-interactor/client/node"
	protos "github.com/coherentopensource/evm-etl/protos/go/protos/chains/optimism"
	"github.com/coherentopensource/go-service-framework/util"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	traceKnownError = "TypeError: cannot read property 'toString' of undefined    in server-side tracer function 'result'"
)

type client struct {
	innerClient node.Client
	logger      util.Logger
}

// EthBlockNumber gets the most recent block number
func (c *client) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	number, err := c.innerClient.GetLatestBlockNumber(ctx)
	if err != nil {
		return 0, err
	}
	return number, nil
}

// GetBlockByNumber gets a block by number
func (c *client) GetBlockByNumber(ctx context.Context, blockNumber uint64) (*protos.Block, error) {
	res, err := c.innerClient.GetBlockByNumber(ctx, blockNumber)
	if err != nil {
		return nil, err
	}

	data := &protos.Block{}
	if err := protojson.Unmarshal(res.Result, data); err != nil {
		return nil, err
	}

	return data, nil
}

func (c *client) GetTracesForBlock(ctx context.Context, blockNumber uint64) ([]*protos.CallTrace, error) {
	// genesis block has no traces
	if blockNumber == 0 {
		return nil, nil
	}

	res, err := c.innerClient.GetTracesForBlock(ctx, blockNumber)
	if err != nil {

		//	Special case where known error should fail gracefully
		if err.Error() == traceKnownError {
			c.logger.Warnf("Known trace error encountered; defaulting to empty traces response: %v", err)
			return []*protos.CallTrace{}, nil
		}

		return nil, err
	}

	var rawTraces []*protos.CallTrace
	for _, trace := range res.Result {
		if trace.Error != nil {
			return nil, fmt.Errorf("%v", trace.Error)
		}
		rawTrace := &protos.CallTrace{}
		if err := protojson.Unmarshal(trace.Result, rawTrace); err != nil {
			return nil, err
		}
		rawTraces = append(rawTraces, rawTrace)
	}

	return rawTraces, nil
}

func (c *client) GetTransactionReceipt(ctx context.Context, txHash string) (*protos.TransactionReceipt, error) {
	res, err := c.innerClient.GetTransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, err
	}

	rawReceipt := &protos.TransactionReceipt{}
	if err := protojson.Unmarshal(res.Result, rawReceipt); err != nil {
		return nil, err
	}

	return rawReceipt, nil
}
