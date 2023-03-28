package ethereum

import (
	"context"
	nodeClient "github.com/datadaodevs/evm-etl/client/node"
	"github.com/datadaodevs/go-service-framework/pool"
)

const (
	stageFetchBlock   = "fetch.block"
	stageFetchReceipt = "fetch.receipt"
	stageFetchTraces  = "fetch.traces"
)

type tmpConfig struct {
	GCPProjectID   string
	BucketName     string
	DirectoryRange int
}

type EthereumDriver struct {
	client nodeClient.Client
	logger utils.Logger
	config *tmpConfig
}

func New(nodeClient nodeClient.Client, logger utils.Logger) *EthereumDriver {
	return &EthereumDriver{
		client: nodeClient,
		logger: logger,
		config: &tmpConfig{
			GCPProjectID:   "rosetta-352219",
			BucketName:     "coherent-test-new-poller-eth",
			DirectoryRange: 10000,
		},
	}
}

func (e *EthereumDriver) FetchSequence(index uint64) map[string]pool.Runner {
	return map[string]pool.Runner{
		stageFetchBlock:   e.queueGetBlockByNumber(index),
		stageFetchReceipt: e.queueGetBlockReceiptsByNumber(index),
		stageFetchTraces:  e.queueGetBlockTraceByNumber(index),
	}
}

func (e *EthereumDriver) Blockchain() string {
	return "ethereum"
}

func (e *EthereumDriver) queueGetBlockTraceByNumber(index uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockTraceByNumber(ctx, index)
	}
}

func (e *EthereumDriver) queueGetBlockByNumber(index uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockByNumber(ctx, index)
	}
}

func (e *EthereumDriver) queueGetBlockReceiptsByNumber(index uint64) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		return e.getBlockReceiptsByNumber(ctx, index)
	}
}
