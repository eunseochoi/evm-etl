package arbitrum

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	protos "github.com/coherentopensource/chain-interactor/protos/go/protos/chains/arbitrum"
	model "github.com/coherentopensource/evm-etl/model/arbitrum"
)

// ProtoBlockToParquet converts a block proto to parquet
func ProtoBlockToParquet(in *protos.Block) *model.ParquetBlock {
	out := model.ParquetBlock{
		Number:           in.Number,
		Hash:             in.Hash,
		ParentHash:       in.ParentHash,
		Nonce:            in.Nonce,
		SHA3Uncles:       in.Sha3Uncles,
		LogsBloom:        in.LogsBloom,
		TransactionsRoot: in.TransactionsRoot,
		StateRoot:        in.StateRoot,
		ReceiptsRoot:     in.ReceiptsRoot,
		Miner:            in.Miner,
		Difficulty:       in.Difficulty,
		TotalDifficulty:  in.TotalDifficulty,
		ExtraData:        in.ExtraData,
		Size:             in.Size,
		GasLimit:         in.GasLimit,
		GasUsed:          in.GasUsed,
		Timestamp:        in.Timestamp,
		MixHash:          in.MixHash,
	}

	for _, uncle := range in.Uncles {
		out.Uncles = append(out.Uncles, uncle)
	}

	return &out
}

// ProtoTransactionToParquet converts a transaction proto to parquet, given a transaction and receipt
func ProtoTransactionToParquet(inTx *protos.Transaction, inReceipt *protos.TransactionReceipt) (*model.ParquetTransaction, error) {
	out := model.ParquetTransaction{
		BlockNumber:       inTx.BlockNumber,
		BlockHash:         inTx.BlockHash,
		Hash:              inTx.Hash,
		From:              inTx.From,
		To:                inTx.To,
		Value:             inTx.Value,
		Gas:               inTx.Gas,
		GasPrice:          inTx.GasPrice,
		Input:             inTx.Input,
		Nonce:             inTx.Nonce,
		TransactionIndex:  inTx.TransactionIndex,
		V:                 inTx.V,
		R:                 inTx.R,
		S:                 inTx.S,
		CumulativeGasUsed: inReceipt.CumulativeGasUsed,
		GasUsed:           inReceipt.GasUsed,
		LogsBloom:         inReceipt.LogsBloom,
		Status:            inReceipt.Status,
		L1Fee:             inReceipt.L1Fee,
		L1FeeScalar:       inReceipt.L1FeeScalar,
		L1GasPrice:        inReceipt.L1GasPrice,
		L1GasUsed:         inReceipt.L1GasUsed,
	}

	return &out, nil
}

// ProtoLogToParquet converts a log proto to parquet
func ProtoLogToParquet(in *protos.Log) *model.ParquetLog {
	out := model.ParquetLog{
		BlockNumber:      in.BlockNumber,
		BlockHash:        in.BlockHash,
		TransactionHash:  in.TransactionHash,
		TransactionIndex: in.TransactionIndex,
		LogIndex:         in.LogIndex,
		Address:          in.Address,
		Data:             in.Data,
		Removed:          in.Removed,
	}
	for _, topic := range in.Topics {
		out.Topics = append(out.Topics, topic)
	}

	return &out
}

// ProtoTraceToParquet converts a trace proto to parquet, given a trace, a transaction, and other supplemental data
func ProtoTraceToParquet(inTrace *protos.CallTrace, inTransaction *protos.Transaction, hash string, parentHash string, index int64) *model.ParquetTrace {
	return &model.ParquetTrace{
		BlockNumber:     inTransaction.BlockNumber,
		BlockHash:       inTransaction.BlockHash,
		TransactionHash: inTransaction.Hash,
		Hash:            hash,
		ParentHash:      parentHash,
		Index:           index,
		Type:            inTrace.Result.Type,
		From:            inTrace.Result.From,
		To:              inTrace.Result.To,
		Value:           inTrace.Result.Value,
		Gas:             inTrace.Result.Gas,
		GasUsed:         inTrace.Result.GasUsed,
		Input:           inTrace.Result.Input,
		Output:          inTrace.Result.Output,
	}
}

func ProtoEVMTransferToParquet(in *protos.Transfer, inTransaction *protos.Transaction, hash string, sequence string, transactionIndex int64, index int64) *model.ParquetEVMTransfer {
	return &model.ParquetEVMTransfer{
		BlockNumber:      inTransaction.BlockNumber,
		BlockHash:        inTransaction.BlockHash,
		TransactionHash:  inTransaction.Hash,
		TraceHash:        hash,
		From:             in.From,
		To:               in.To,
		Value:            in.Value,
		Sequence:         sequence,
		TransactionIndex: transactionIndex,
		Index:            index,
	}
}

// hashCallTrace hashes a trace proto
func hashCallTrace(callTrace *protos.CallTrace) string {
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%v", callTrace)))

	return hex.EncodeToString(hasher.Sum(nil))
}
