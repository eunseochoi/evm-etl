package binance

import (
	nodeClient "github.com/coherentopensource/evm-etl/client/node"
	"github.com/coherentopensource/evm-etl/shared/storage"
	"github.com/coherentopensource/go-service-framework/constants"
	"github.com/coherentopensource/go-service-framework/util"
)

const (
	stageFetchBlock   = "fetch.block"
	stageFetchReceipt = "fetch.receipt"
	stageFetchTraces  = "fetch.traces"
)

// Driver is the container for all ETL business logic
type Driver struct {
	store      *store
	nodeClient *client
	logger     util.Logger
	config     *Config
}

// NewDriver constructs a new Driver
func NewDriver(cfg *Config, nodeClient nodeClient.Client, innerStore storage.Store, logger util.Logger) *Driver {
	return &Driver{
		nodeClient: &client{innerClient: nodeClient},
		store:      &store{innerStore: innerStore},
		logger:     logger,
		config:     cfg,
	}
}

// Blockchain returns the name of the blockchain
func (d *Driver) Blockchain() string {
	return string(constants.Binance_Smart_Chain)
}
