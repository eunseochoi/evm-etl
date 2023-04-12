package binance

// ParquetBlock represents a block in parquet form
type ParquetBlock struct {
	Number           string   `parquet:"name=block_number, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Hash             string   `parquet:"name=block_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ParentHash       string   `parquet:"name=parent_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Nonce            string   `parquet:"name=nonce, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SHA3Uncles       string   `parquet:"name=sha3_uncles, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LogsBloom        string   `parquet:"name=logs_bloom, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionsRoot string   `parquet:"name=transactions_root, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	StateRoot        string   `parquet:"name=state_root, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ReceiptsRoot     string   `parquet:"name=receipts_root, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Miner            string   `parquet:"name=miner, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Difficulty       string   `parquet:"name=difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TotalDifficulty  string   `parquet:"name=total_difficulty, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ExtraData        string   `parquet:"name=extra_data, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Size             string   `parquet:"name=size, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasLimit         string   `parquet:"name=gas_limit, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasUsed          string   `parquet:"name=gas_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp        string   `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Uncles           []string `parquet:"name=uncles, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	BaseFeePerGas    string   `parquet:"name=base_fee_per_gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MixHash          string   `parquet:"name=mix_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// ParquetTransaction represents a transaction in parquet form
type ParquetTransaction struct {
	BlockNumber          string   `parquet:"name=block_number, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BlockHash            string   `parquet:"name=block_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Hash                 string   `parquet:"name=transaction_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	From                 string   `parquet:"name=from_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	To                   string   `parquet:"name=to_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value                string   `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Gas                  string   `parquet:"name=gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasPrice             string   `parquet:"name=gas_price, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Input                string   `parquet:"name=input, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type                 string   `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Nonce                string   `parquet:"name=nonce, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionIndex     string   `parquet:"name=transaction_index, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	V                    string   `parquet:"name=v, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	R                    string   `parquet:"name=r, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	S                    string   `parquet:"name=s, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CumulativeGasUsed    string   `parquet:"name=cumulative_gas_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EffectiveGasPrice    string   `parquet:"name=effective_gas_price, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MaxFeePerGas         string   `parquet:"name=max_fee_per_gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MaxPriorityFeePerGas string   `parquet:"name=max_priority_fee_per_gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasUsed              string   `parquet:"name=gas_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LogsBloom            string   `parquet:"name=logs_bloom, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Status               string   `parquet:"name=status, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AccessList           []string `parquet:"name=access_list, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

// ParquetLog represents a log in parquet form
type ParquetLog struct {
	BlockNumber      string   `parquet:"name=block_number, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BlockHash        string   `parquet:"name=block_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionHash  string   `parquet:"name=transaction_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionIndex string   `parquet:"name=transaction_index, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LogIndex         string   `parquet:"name=log_index, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Address          string   `parquet:"name=address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Data             string   `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Topics           []string `parquet:"name=topics, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Removed          bool     `parquet:"name=removed, type=BOOLEAN"`
}

// ParquetTrace represents a trace in parquet form
type ParquetTrace struct {
	BlockNumber     string `parquet:"name=block_number, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BlockHash       string `parquet:"name=block_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionHash string `parquet:"name=transaction_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Hash            string `parquet:"name=trace_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ParentHash      string `parquet:"name=parent_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Index           int64  `parquet:"name=trace_index, type=INT64"`
	Type            string `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	From            string `parquet:"name=from_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	To              string `parquet:"name=to_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Value           string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Gas             string `parquet:"name=gas, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	GasUsed         string `parquet:"name=gas_used, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Input           string `parquet:"name=input, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Output          string `parquet:"name=output, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Error           string `parquet:"name=error, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RevertReason    string `parquet:"name=revert_reason, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}
