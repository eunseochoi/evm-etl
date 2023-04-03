package ethereum

import (
	nodeClient "github.com/datadaodevs/evm-etl/client/node"
	"github.com/datadaodevs/evm-etl/shared/storage"
	"github.com/datadaodevs/go-service-framework/constants"
	"github.com/datadaodevs/go-service-framework/util"
)

const (
	stageFetchBlock   = "fetch.block"
	stageFetchReceipt = "fetch.receipt"
	stageFetchTraces  = "fetch.traces"
)

// EthereumDriver is the container for all ETL business logic
type EthereumDriver struct {
	store      storage.Store
	nodeClient nodeClient.Client
	logger     util.Logger
	config     *Config
}

// New constructs a new EthereumDriver
func New(cfg *Config, nodeClient nodeClient.Client, store storage.Store, logger util.Logger) *EthereumDriver {
	return &EthereumDriver{
		nodeClient: nodeClient,
		store:      store,
		logger:     logger,
		config:     cfg,
	}
}

// Blockchain returns the name of the blockchain
func (e *EthereumDriver) Blockchain() string {
	return string(constants.Ethereum)
}
