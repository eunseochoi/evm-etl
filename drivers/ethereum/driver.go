package ethereum

import (
	nodeClient "github.com/datadaodevs/evm-etl/client/node"
	"github.com/datadaodevs/go-service-framework/util"
)

const (
	stageFetchBlock   = "fetch.block"
	stageFetchReceipt = "fetch.receipt"
	stageFetchTraces  = "fetch.traces"
)

// Config stores configurable properties of the driver
type Config struct {
	GCPProjectID   string
	BucketName     string
	DirectoryRange int
}

// EthereumDriver is the container for all ETL business logic
type EthereumDriver struct {
	client nodeClient.Client
	logger util.Logger
	config *Config
}

// New constructs a new EthereumDriver
func New(cfg *Config, nodeClient nodeClient.Client, logger util.Logger) *EthereumDriver {
	return &EthereumDriver{
		client: nodeClient,
		logger: logger,
		config: cfg,
	}
}

// Blockchain returns the name of the blockchain
func (e *EthereumDriver) Blockchain() string {
	return "ethereum"
}
