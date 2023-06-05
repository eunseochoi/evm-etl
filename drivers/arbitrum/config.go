package arbitrum

import (
	"github.com/caarlos0/env/v7"
	"github.com/coherentopensource/go-service-framework/util"
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
		logger.Fatalf("Could not parse Arbitrum driver config: %v", err)
	}

	return &cfg
}
