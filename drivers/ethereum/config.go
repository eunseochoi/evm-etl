package ethereum

import (
	"github.com/caarlos0/env/v7"
	"github.com/datadaodevs/go-service-framework/util"
)

// Config stores configurable properties of the driver
type Config struct {
	MaxRetries     int `env:"http_max_retries" envDefault:"10"`
	DirectoryRange int `env:"bucket_directory_range" envDefault:"10000"`
}

// MustParseConfig uses env.Parse to initialize config with environment variables
func MustParseConfig(logger util.Logger) *Config {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		logger.Fatalf("Could not parse Ethereum driver config: %v", err)
	}

	return &cfg
}
