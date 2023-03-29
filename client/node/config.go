package node

import (
	"github.com/caarlos0/env/v7"
	"github.com/datadaodevs/go-service-framework/constants"
	"github.com/datadaodevs/go-service-framework/util"
	"time"
)

// Config holds configurable properties for node client
type Config struct {
	Blockchain                constants.Blockchain `env:"blockchain,required"`
	NodeHost                  string               `env:"node_host,required"`
	EnrichTransactionsTimeout time.Duration        `env:"enrich_transactions_timeout" envDefault:"14s"`
	FetchBlockTimeout         time.Duration        `env:"fetch_block_timeout" envDefault:"14s"`
	RPCTimeout                time.Duration        `env:"rpc_timeout" envDefault:"20000ms"`
	RPCRetries                int                  `env:"rpc_retries" envDefault:"2"`
}

// ParseConfig parses config from env vars
func ParseConfig() (*Config, error) {
	var cfg Config

	if err := env.Parse(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// MustParseConfig parses config from env vars, with fatal exit on error
func MustParseConfig(logger util.Logger) *Config {
	cfg, err := ParseConfig()
	if err != nil {
		logger.Fatalf("Failed to parse node client config: %v", err)
	}

	return cfg
}
