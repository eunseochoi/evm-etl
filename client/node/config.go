package node

import (
	"github.com/caarlos0/env/v7"
	"github.com/datadaodevs/go-service-framework/constants"
	"github.com/datadaodevs/go-service-framework/util"
	"time"
)

// Config holds configurable properties for node client
type Config struct {
	Blockchain constants.Blockchain `env:"BLOCKCHAIN,required"`
	NodeHost   string               `env:"NODE_HOST,required"`
	RPCTimeout time.Duration        `env:"RPC_TIMEOUT" envDefault:"300s"`
	RPCRetries int                  `env:"RPC_RETRIES" envDefault:"2"`
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
