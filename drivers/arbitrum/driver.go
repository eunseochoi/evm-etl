package arbitrum

import (
	nodeClient "github.com/coherentopensource/chain-interactor/client/node"
	"github.com/coherentopensource/evm-etl/shared/storage"
	"github.com/coherentopensource/go-service-framework/constants"
	"github.com/coherentopensource/go-service-framework/util"
)

const (
	stageFetchBlock  = "fetch.block"
	stageFetchTraces = "fetch.traces"
)

// Driver is the container for all ETL business logic
type Driver struct {
	store      *store
	nodeClient *client
	logger     util.Logger
	config     *Config
}

// New constructs a new Driver
func New(cfg *Config, nodeClient nodeClient.Client, innerStore storage.Store, logger util.Logger) *Driver {
	return &Driver{
		nodeClient: &client{innerClient: nodeClient, logger: logger},
		store:      &store{innerStore: innerStore},
		logger:     logger,
		config:     cfg,
	}
}

// Blockchain returns the name of the blockchain
func (d *Driver) Blockchain() string {
	return string(constants.Arbitrum)
}
