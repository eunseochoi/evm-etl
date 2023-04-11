package binance

import (
	"github.com/caarlos0/env/v7"
	"github.com/datadaodevs/go-service-framework/util"
)

// Config stores configurable properties of the driver
type Config struct {
	MaxRetries     int    `env:"HTTP_MAX_RETRIES" envDefault:"10"`
	DirectoryRange uint64 `env:"BUCKET_DIRECTORY_RANGE" envDefault:"10000"`
}

// MustParseConfig uses env.Parse to initialize config with environment variables
func MustParseConfig(logger util.Logger) *Config {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		logger.Fatalf("could not parse Binance Smart Chain driver config: %v", err)
	}

	return &cfg
}
