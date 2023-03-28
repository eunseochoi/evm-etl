package node

import (
	"github.com/datadaodevs/go-service-framework/constants"
	"github.com/spf13/viper"
	"time"
)

type Config struct {
	Blockchain constants.Blockchain

	EthNodeRPC                string
	PolyNodeRPC               string
	GoerliNodeRPC             string
	OptNodeRPC                string
	EnrichTransactionsTimeout time.Duration

	FetchBlockTimeout time.Duration
}

func NewConfig() *Config {
	setDefaults()

	viper.AutomaticEnv()
	config := Config{
		Blockchain:                constants.Blockchain(viper.GetString("blockchain")),
		EthNodeRPC:                viper.GetString("ethereum_node_rpc_endpoint"),
		OptNodeRPC:                viper.GetString("optimism_node_rpc_endpoint"),
		PolyNodeRPC:               viper.GetString("polygon_node_rpc_endpoint"),
		GoerliNodeRPC:             viper.GetString("goerli_node_rpc_endpoint"),
		EnrichTransactionsTimeout: viper.GetDuration("enrich_transactions_timeout"),
		FetchBlockTimeout:         viper.GetDuration("fetch_block_timeout"),
	}

	return &config
}

func setDefaults() {
	viper.SetDefault("ethereum_node_rpc_endpoint", "https://withered-red-thunder.quiknode.pro/8b87fd6f08cdd14442dc22fbe7bd7a4d1ba8b94a/")
	viper.SetDefault("optimism_node_rpc_endpoint", "https://ultra-proud-emerald.optimism.quiknode.pro/6d6d3edf4f0b58d5d24b3847ce479fe247f642cf/")
	viper.SetDefault("polygon_node_rpc_endpoint", "https://wider-fabled-wildflower.matic.quiknode.pro/60af747f09cd941ca046f0f4a90ddb65ee90cb96/")
	viper.SetDefault("goerli_node_rpc_endpoint", "https://chaotic-nameless-night.ethereum-goerli.quiknode.pro/626d2a56cc59d9f182e2725d1acd4861a38888ce/")
	viper.SetDefault("rpc_retries", 2)
	viper.SetDefault("rpc_timeout", "20000ms")
	viper.SetDefault("fetch_block_timeout", "14s")
	viper.SetDefault("blockchain", "ethereum")
}
